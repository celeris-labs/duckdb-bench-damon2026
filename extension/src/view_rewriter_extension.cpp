#define DUCKDB_EXTENSION_MAIN

#include "view_rewriter_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <sstream>

namespace duckdb {

// ──────────────────────────────────────────────────────────────────────
// RewriteRule: describes how to match a FILTER(SCAN(table)) pattern
// and what pre-filtered view name to substitute.
// ──────────────────────────────────────────────────────────────────────
struct RewriteRule {
	//! The source table name to match (e.g., "lineitem")
	string source_table;
	//! A human-readable description of the filter predicate to match.
	//! The actual matching is done via a fingerprint string.
	string filter_description;
	//! The fingerprint of the filter expression tree (serialized form)
	//! Used for exact structural matching.
	string filter_fingerprint;
	//! The name of the pre-filtered view/table to replace the scan with
	string replacement_view;
};

// ──────────────────────────────────────────────────────────────────────
// Extension info: holds registered rewrite rules and statistics
// ──────────────────────────────────────────────────────────────────────
struct ViewRewriterInfo : public OptimizerExtensionInfo {
	vector<RewriteRule> rules;
	idx_t rewrites_applied = 0;
	bool verbose = false;
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

static void CollectPushedDownFilters(vector<string> &column_names, TableFilterSet &filters, vector<string> &fps) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		// Build a canonical string representation of the filter
		// e.g., "col_3 >= 5" or "col_7 = 'BUILDING'"
		string fp = filter->ToString(column_names[column_index]);
		fps.push_back(fp);
	}
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
		}
	}
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
			rule.filter_description = filter_fingerprint;
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

	OptimizerExtension opt_ext;
	opt_ext.optimize_function = ViewRewriterOptimize;
	opt_ext.optimizer_info = make_shared_ptr<ViewRewriterInfo>();
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
