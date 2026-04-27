#define DUCKDB_EXTENSION_MAIN

#include "view_rewriter_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <mutex>
#include <sstream>
#include <unordered_set>

namespace duckdb {

// ──────────────────────────────────────────────────────────────────────
// RewriteRule: describes how to match a FILTER(SCAN(table)) pattern
// and what pre-filtered view name to substitute.
// ──────────────────────────────────────────────────────────────────────
struct RewriteRule {
	//! The source table name to match (e.g., "lineitem")
	string source_table;
	//! The fingerprint of the filter expression tree (serialized form)
	//! Used for exact structural matching.
	string filter_fingerprint;
	//! The name of the pre-filtered view/table to replace the scan with
	string replacement_view;
};

// ──────────────────────────────────────────────────────────────────────
// Extension info: holds registered rewrite rules and statistics
// ──────────────────────────────────────────────────────────────────────
struct PendingMaterialization {
	string create_sql;
	RewriteRule rule;
};

struct ViewRewriterInfo : public OptimizerExtensionInfo {
	vector<RewriteRule> rules;
	idx_t rewrites_applied = 0;
	bool verbose = false;
	bool auto_materialize = false;

	// Pending materializations enqueued during the optimizer pass.
	// Executed after the triggering query completes, on a side connection.
	std::mutex pending_mutex;
	vector<PendingMaterialization> pending;
	// Fingerprints already enqueued (but not yet materialized), to avoid duplicates.
	unordered_set<string> enqueued_fingerprints;

	// Held so we can open a side connection for materialization.
	shared_ptr<DatabaseInstance> db;
};

// Drains pending materializations in QueryEnd via a side connection.
// QueryEnd fires while the caller's ClientContextLock is still held, so we
// cannot call context.Query() — that would deadlock. A fresh Connection on
// the same DatabaseInstance has its own lock and is safe to use.
struct AutoMaterializeState : public ClientContextState {
	ViewRewriterInfo &info;

	explicit AutoMaterializeState(ViewRewriterInfo &info) : info(info) {}

	void QueryEnd(ClientContext &context) override {
		vector<PendingMaterialization> work;
		{
			std::lock_guard<std::mutex> lk(info.pending_mutex);
			work.swap(info.pending);
		}
		if (work.empty()) {
			return;
		}

		// Side connection — its own lock, no deadlock with the caller.
		Connection side_con(*info.db);

		for (auto &pm : work) {
			if (info.verbose) {
				Printer::Print("  Auto-materializing: " + pm.create_sql);
			}
			auto result = side_con.Query(pm.create_sql);
			if (result->HasError()) {
				if (info.verbose) {
					Printer::Print("  Auto-materialize failed: " + result->GetError());
				}
				std::lock_guard<std::mutex> lk(info.pending_mutex);
				info.enqueued_fingerprints.erase(pm.rule.filter_fingerprint);
				continue;
			}
			{
				std::lock_guard<std::mutex> lk(info.pending_mutex);
				info.rules.push_back(pm.rule);
				info.enqueued_fingerprints.erase(pm.rule.filter_fingerprint);
			}
			if (info.verbose) {
				Printer::Print("  Auto-materialized '" + pm.rule.replacement_view +
				               "'; rule registered for future queries.");
			}
		}
	}
};

// ──────────────────────────────────────────────────────────────────────
// Expression fingerprinting: serialize an expression tree to a canonical
// string for structural comparison. Column references use binding info
// so that column identity is preserved.
// ──────────────────────────────────────────────────────────────────────
static string FingerprintExpression(const Expression &expr) {
	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		return "(" + ExpressionTypeToString(expr.type) + " " +
		       FingerprintExpression(*comp.left) + " " +
		       FingerprintExpression(*comp.right) + ")";
	}
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		// Sort children for canonical form (AND/OR are commutative)
		vector<string> child_fps;
		for (auto &child : conj.children) {
			child_fps.push_back(FingerprintExpression(*child));
		}
		sort(child_fps.begin(), child_fps.end());
		string result = "(" + ExpressionTypeToString(expr.type);
		for (auto &fp : child_fps) {
			result += " " + fp;
		}
		result += ")";
		return result;
	}
	case ExpressionType::VALUE_CONSTANT: {
		auto &constant = expr.Cast<BoundConstantExpression>();
		return "CONST:" + constant.value.ToString();
	}
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		// Use the alias (column name) for readability + matching
		return "COL:" + colref.alias;
	}
	case ExpressionType::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		string result = "FN:" + func.function.name + "(";
		for (idx_t i = 0; i < func.children.size(); i++) {
			if (i > 0) result += ",";
			result += FingerprintExpression(*func.children[i]);
		}
		result += ")";
		return result;
	}
	default:
		return "EXPR:" + expr.ToString();
	}
}

// ──────────────────────────────────────────────────────────────────────
// Collect all filter expressions from a LogicalFilter node.
// Handles conjunction (AND) by flattening.
// ──────────────────────────────────────────────────────────────────────
static void CollectFilterExpressions(const vector<unique_ptr<Expression>> &expressions,
                                      vector<string> &fingerprints) {
	for (auto &expr : expressions) {
		fingerprints.push_back(FingerprintExpression(*expr));
	}
}

// ──────────────────────────────────────────────────────────────────────
// Get the table name from a LogicalGet operator
// ──────────────────────────────────────────────────────────────────────
static string GetTableName(const LogicalGet &get) {
	// The function name is typically "seq_scan" for table scans
	// The table name can be retrieved from the bind data
	return get.GetTable() ? get.GetTable()->name : get.function.name;
}

// ──────────────────────────────────────────────────────────────────────
// Core plan rewriter: recursively walks the logical plan tree
// looking for LogicalGet nodes with pushed-down filters that match
// registered rules. When a match is found, replaces the LogicalGet
// with a new LogicalGet that scans the pre-filtered replacement view.
// ──────────────────────────────────────────────────────────────────────

// Render a TableFilter to a valid SQL expression string.
// Children of conjunction filters are sorted for a canonical form so that
// fingerprints are stable regardless of the order DuckDB emits them.
// OptionalFilter is transparent — we render its child as-is.
static string FilterToSQL(const TableFilter &filter, const string &col) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &cf = filter.Cast<ConstantFilter>();
		return col + ExpressionTypeToOperator(cf.comparison_type) + cf.constant.ToSQLString();
	}
	case TableFilterType::IS_NULL:
		return col + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return col + " IS NOT NULL";
	case TableFilterType::IN_FILTER: {
		auto &inf = filter.Cast<InFilter>();
		string result = col + " IN (";
		vector<string> vals;
		for (auto &v : inf.values) {
			vals.push_back(v.ToSQLString());
		}
		sort(vals.begin(), vals.end());
		for (idx_t i = 0; i < vals.size(); i++) {
			if (i > 0) result += ", ";
			result += vals[i];
		}
		return result + ")";
	}
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR: {
		bool is_or = filter.filter_type == TableFilterType::CONJUNCTION_OR;
		auto &conj = is_or ? filter.Cast<ConjunctionOrFilter>()
		                   : static_cast<const ConjunctionFilter &>(filter.Cast<ConjunctionAndFilter>());
		vector<string> parts;
		for (auto &child : conj.child_filters) {
			parts.push_back(FilterToSQL(*child, col));
		}
		sort(parts.begin(), parts.end());
		string result = "(";
		for (idx_t i = 0; i < parts.size(); i++) {
			if (i > 0) result += is_or ? " OR " : " AND ";
			result += parts[i];
		}
		return result + ")";
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &opt = filter.Cast<OptionalFilter>();
		if (opt.child_filter) {
			return FilterToSQL(*opt.child_filter, col);
		}
		return "";
	}
	default:
		// Fallback: not safe to materialize unknown filter types
		return "";
	}
}

static void CollectPushedDownFilters(vector<string> &column_names, TableFilterSet &filters, vector<string> &fps) {
	for (auto &entry : filters.filters) {
		string sql = FilterToSQL(*entry.second, column_names[entry.first]);
		if (!sql.empty()) {
			fps.push_back(sql);
		}
	}
}

static string FilterSetToSQL(vector<string> &column_names, TableFilterSet &filters) {
	vector<string> clauses;
	for (auto &entry : filters.filters) {
		string sql = FilterToSQL(*entry.second, column_names[entry.first]);
		if (!sql.empty()) {
			clauses.push_back(sql);
		}
	}
	sort(clauses.begin(), clauses.end());
	string result;
	for (auto &c : clauses) {
		if (!result.empty()) result += " AND ";
		result += c;
	}
	return result;
}

static void RewritePlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                        ViewRewriterInfo *info) {
	// Recurse into children first (bottom-up)
	for (auto &child : op->children) {
		RewritePlan(context, child, info);
	}

	if (op->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = op->Cast<LogicalGet>();

	string table_name;
	try {
		table_name = GetTableName(get);
	} catch (...) {
		return;
	}

	if (table_name.empty()) {
		return;
	}

	// Build fingerprint from the pushed-down filters on this GET node
	vector<string> filter_fps;
    CollectPushedDownFilters(get.names, get.table_filters, filter_fps);

	if (filter_fps.empty()) {
		return;
	}

	sort(filter_fps.begin(), filter_fps.end());

	string combined_fp;
	for (auto &fp : filter_fps) {
		if (!combined_fp.empty()) combined_fp += " AND ";
		combined_fp += fp;
	}

	if (info->verbose) {
		Printer::Print("ViewRewriter: Examining GET on table '" + table_name +
		               "' with pushed-down filters");
		Printer::Print("  Fingerprint: " + combined_fp);
	}

	// Check against registered rules
	for (auto &rule : info->rules) {
		if (!StringUtil::CIEquals(rule.source_table, table_name)) {
			continue;
		}
		if (rule.filter_fingerprint != combined_fp) {
			continue;
		}

		if (info->verbose) {
			Printer::Print("  MATCH! Rewriting to view '" + rule.replacement_view + "'");
		}

		try {
			// Look up the replacement view in the catalog
			if (!get.GetTable()) {
				continue;
			}
			auto &catalog = get.GetTable()->catalog;
			string schema_name = get.GetTable()->GetInfo()->schema;

			auto replacement_entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY,
                schema_name, rule.replacement_view, OnEntryNotFound::RETURN_NULL);

			if (!replacement_entry) {
				if (info->verbose) {
					Printer::Print("  WARNING: Replacement '" + rule.replacement_view +
					               "' not found, skipping.");
				}
				continue;
			}

            auto &table_entry = replacement_entry->Cast<DuckTableEntry>();

            // Construct the new LogicalGet directly
            unique_ptr<FunctionData> bind_data;
			auto table_function = table_entry.GetScanFunction(context, bind_data);

			auto new_get = make_uniq<LogicalGet>(get.table_index, table_function,
                std::move(bind_data), get.returned_types, get.names, get.virtual_columns);
            new_get->projection_ids = get.projection_ids;
            auto column_ids = get.GetColumnIds();
            new_get->SetColumnIds(std::move(column_ids));

			// Replace the operator in-place
			op = std::move(new_get);

			info->rewrites_applied++;

			if (info->verbose) {
				Printer::Print("  Rewrite applied: replaced scan of '" + table_name +
				               "' with scan of view '" + rule.replacement_view + "'");
			}
			return;

		} catch (std::exception &e) {
			if (info->verbose) {
				Printer::Print("  ERROR during rewrite: " + string(e.what()));
			}
		} catch (...) {
			// Catch DuckDB internal exceptions that don't derive from std::exception
		}
	}

	// No rule matched — auto-materialize if enabled
	if (!info->auto_materialize) {
		return;
	}

	// Enqueue for execution in QueryEnd — cannot call context.Query() from inside the optimizer
	// (the ClientContextLock is held for the duration of query planning/execution).
	{
		std::lock_guard<std::mutex> lk(info->pending_mutex);
		// Skip if already enqueued for this fingerprint (optimizer may visit the same node twice).
		if (info->enqueued_fingerprints.count(combined_fp)) {
			return;
		}

		PendingMaterialization pm;
		pm.rule.source_table       = table_name;
		pm.rule.filter_fingerprint = combined_fp;
		pm.rule.replacement_view   = table_name + "_auto_" + std::to_string(
		    info->rules.size() + info->enqueued_fingerprints.size());

		string where_sql = FilterSetToSQL(get.names, get.table_filters);
		pm.create_sql = "CREATE TABLE IF NOT EXISTS " + pm.rule.replacement_view +
		                " AS SELECT * FROM " + table_name + " WHERE " + where_sql;

		info->enqueued_fingerprints.insert(combined_fp);
		info->pending.push_back(std::move(pm));
	}

	// Ensure the QueryEnd callback is registered for this context.
	context.registered_state->GetOrCreate<AutoMaterializeState>("view_rewriter_auto_mat", *info);
}

// ──────────────────────────────────────────────────────────────────────
// The optimizer hook function called after DuckDB's built-in optimizers
// ──────────────────────────────────────────────────────────────────────
static void ViewRewriterOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto info = dynamic_cast<ViewRewriterInfo *>(input.info.get());
	if (!info) {
		return;
	}
	RewritePlan(input.context, plan, info);
}

// ──────────────────────────────────────────────────────────────────────
// Table function: view_rewriter_add_rule(source_table, filter_fingerprint, replacement_view)
// Registers a new rewrite rule.
// ──────────────────────────────────────────────────────────────────────
struct AddRuleBindData : public TableFunctionData {
	string message;
};

static unique_ptr<FunctionData> AddRuleBind(ClientContext &context,
                                            TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string> &names) {
	if (input.inputs.size() != 3) {
		throw BinderException("tpch_rewriter_add_rule requires 3 arguments: "
		                      "source_table, filter_fingerprint, replacement_view");
	}

	auto source_table = input.inputs[0].GetValue<string>();
	auto filter_fingerprint = input.inputs[1].GetValue<string>();
	auto replacement_view = input.inputs[2].GetValue<string>();

	// Find our optimizer extension info
	auto &config = DBConfig::GetConfig(context);
	for (auto &ext : config.optimizer_extensions) {
		auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
		if (info) {
			RewriteRule rule;
			rule.source_table = source_table;
			rule.filter_fingerprint = filter_fingerprint;
			rule.replacement_view = replacement_view;
			info->rules.push_back(std::move(rule));
			break;
		}
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");

	auto bind_data = make_uniq<AddRuleBindData>();
	bind_data->message = "Rule added: " + source_table + " -> " + replacement_view;
	return std::move(bind_data);
}

static void AddRuleFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// This is a no-op scan function; the work is done in bind
}

// ──────────────────────────────────────────────────────────────────────
// Table function: view_rewriter_stats()
// Shows how many rewrites have been applied.
// ──────────────────────────────────────────────────────────────────────
struct StatsBindData : public TableFunctionData {
	bool done = false;
};

static unique_ptr<FunctionData> StatsBind(ClientContext &context,
                                          TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types,
                                          vector<string> &names) {
	return_types.push_back(LogicalType::BIGINT);
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rules_registered");
	names.push_back("rewrites_applied");

	return make_uniq<StatsBindData>();
}

static void StatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<StatsBindData>();
	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	idx_t num_rules = 0;
    idx_t rewrites_applied = 0;
	auto &config = DBConfig::GetConfig(context);
	for (auto &ext : config.optimizer_extensions) {
		auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
		if (info) {
			num_rules        = info->rules.size();
            rewrites_applied = info->rewrites_applied;
			break;
		}
	}

	output.SetValue(0, 0, Value::BIGINT(num_rules));
	output.SetValue(1, 0, Value::BIGINT(rewrites_applied));
	output.SetCardinality(1);
	bind_data.done = true;
}

// ══════════════════════════════════════════════════════════════════════
// Extension Load
// ══════════════════════════════════════════════════════════════════════
static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
	// 1. Register the optimizer extension (runs AFTER built-in optimizers)
	auto &config = DBConfig::GetConfig(instance);

	auto info = make_shared_ptr<ViewRewriterInfo>();
	info->db = instance.shared_from_this();

	OptimizerExtension opt_ext;
	opt_ext.optimize_function = ViewRewriterOptimize;
	opt_ext.optimizer_info = std::move(info);
	config.optimizer_extensions.push_back(std::move(opt_ext));

	// 2. Register management table functions
	// view_rewriter_add_rule(source_table, filter_fingerprint, replacement_view)
	TableFunction add_rule_func("view_rewriter_add_rule",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    AddRuleFunction, AddRuleBind);
	loader.RegisterFunction(add_rule_func);

	// view_rewriter_stats()
	TableFunction stats_func("view_rewriter_stats", {}, StatsFunction, StatsBind);
	loader.RegisterFunction(stats_func);

	// SET view_rewriter_auto_materialize = true/false
	config.AddExtensionOption(
	    "view_rewriter_auto_materialize",
	    "Automatically materialize filter sub-expressions as in-memory tables and register rewrite rules",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false),
	    [](ClientContext &context, SetScope scope, Value &value) {
		    auto &cfg = DBConfig::GetConfig(context);
		    for (auto &ext : cfg.optimizer_extensions) {
			    auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
			    if (info) {
				    info->auto_materialize = BooleanValue::Get(value);
				    break;
			    }
		    }
	    });
}

void ViewRewriterExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ViewRewriterExtension::Name() {
	return "view_rewriter";
}

std::string ViewRewriterExtension::Version() const {
#ifdef EXT_VERSION_VIEW_REWRITER
	return EXT_VERSION_VIEW_REWRITER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(view_rewriter, loader) {
	duckdb::LoadInternal(loader);
}

}
