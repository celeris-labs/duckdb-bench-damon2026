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
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
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
    string      where_sql;
    RewriteRule rule;
};

struct ViewRewriterInfo : public OptimizerExtensionInfo {
    vector<RewriteRule> rules;
    idx_t               rewrites_applied = 0;
    bool                verbose          = false;
    bool                auto_materialize = false;
    double              min_selectivity  = 0.0;

    // Pending materializations enqueued during the optimizer pass.
    // Executed after the triggering query completes, on a side connection.
    std::mutex                     pending_mutex;
    vector<PendingMaterialization> pending;
    // Fingerprints we have ever seen (enqueued, failed, or skipped). Never retry these.
    unordered_set<string> seen_fingerprints;

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
            // Compute selectivity: count(filtered) / count(table)
            auto table_count_result =
                side_con.Query("SELECT count(*) FROM " + pm.rule.source_table);
            auto view_count_result = side_con.Query("SELECT count(*) FROM " + pm.rule.source_table +
                                                    " WHERE " + pm.where_sql);
            double selectivity     = 1.0;
            if (!table_count_result->HasError() && !view_count_result->HasError()) {
                auto table_row = table_count_result->Fetch();
                auto view_row  = view_count_result->Fetch();
                if (table_row && view_row) {
                    idx_t table_card = table_row->GetValue(0, 0).GetValue<idx_t>();
                    idx_t view_card  = view_row->GetValue(0, 0).GetValue<idx_t>();
                    selectivity =
                        table_card > 0 ? 1.0 - (double)view_card / (double)table_card : 1.0;
                    if (info.verbose) {
                        Printer::Print("  Selectivity for view '" + pm.rule.replacement_view +
                                       "': 1 - " + std::to_string(view_card) + " / " +
                                       std::to_string(table_card) + " = " +
                                       std::to_string(selectivity));
                    }
                }
            }

            if (selectivity < info.min_selectivity) {
                if (info.verbose) {
                    Printer::Print("  Skipping materialization: selectivity " +
                                   std::to_string(selectivity) + " < " +
                                   std::to_string(info.min_selectivity));
                }
                continue;
            }

            string create_sql = "CREATE TABLE IF NOT EXISTS " + pm.rule.replacement_view +
                                " AS SELECT * FROM " + pm.rule.source_table + " WHERE " +
                                pm.where_sql;

            if (info.verbose) {
                Printer::Print("  Auto-materializing: " + create_sql);
            }
            auto result = side_con.Query(create_sql);
            if (result->HasError()) {
                if (info.verbose) {
                    Printer::Print("  Auto-materialize failed: " + result->GetError());
                }
                continue;
            }
            {
                std::lock_guard<std::mutex> lk(info.pending_mutex);
                info.rules.push_back(pm.rule);
            }
            if (info.verbose) {
                Printer::Print("  Rule already exists and replaces with '" +
                               pm.rule.replacement_view + "'.");
            }
        }
    }
};

// Convert a bound expression to a SQL string for use in CREATE TABLE AS SELECT.
// Conjunction children are sorted for a canonical form used in rule matching.
static string ExpressionToSQL(const Expression &expr) {
    switch (expr.type) {
    case ExpressionType::COMPARE_EQUAL:
    case ExpressionType::COMPARE_NOTEQUAL:
    case ExpressionType::COMPARE_LESSTHAN:
    case ExpressionType::COMPARE_GREATERTHAN:
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
        auto &comp = expr.Cast<BoundComparisonExpression>();
        return ExpressionToSQL(*comp.left) + ExpressionTypeToOperator(expr.type) +
               ExpressionToSQL(*comp.right);
    }
    case ExpressionType::CONJUNCTION_AND:
    case ExpressionType::CONJUNCTION_OR: {
        auto          &conj = expr.Cast<BoundConjunctionExpression>();
        string         op   = (expr.type == ExpressionType::CONJUNCTION_AND) ? " AND " : " OR ";
        vector<string> parts;
        for (auto &child : conj.children) {
            parts.push_back(ExpressionToSQL(*child));
        }
        sort(parts.begin(), parts.end());
        if (parts.size() == 1)
            return parts[0];
        string result = "(";
        for (idx_t i = 0; i < parts.size(); i++) {
            if (i > 0)
                result += op;
            result += parts[i];
        }
        return result + ")";
    }
    case ExpressionType::VALUE_CONSTANT: {
        auto &constant = expr.Cast<BoundConstantExpression>();
        return constant.value.ToSQLString();
    }
    case ExpressionType::BOUND_COLUMN_REF: {
        auto &colref = expr.Cast<BoundColumnRefExpression>();
        return colref.alias;
    }
    case ExpressionType::OPERATOR_CAST: {
        auto &cast = expr.Cast<BoundCastExpression>();
        return "CAST(" + ExpressionToSQL(*cast.child) + " AS " + cast.return_type.ToString() + ")";
    }
    case ExpressionType::BOUND_FUNCTION: {
        auto  &func   = expr.Cast<BoundFunctionExpression>();
        string result = func.function.name + "(";
        for (idx_t i = 0; i < func.children.size(); i++) {
            if (i > 0)
                result += ", ";
            result += ExpressionToSQL(*func.children[i]);
        }
        return result + ")";
    }
    default:
        return expr.ToString();
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
// Returns false if the filter type cannot be expressed as SQL (materialization should be skipped).
// Children of conjunction filters are sorted for a canonical form so that
// fingerprints are stable regardless of the order DuckDB emits them.
// OptionalFilter is transparent — we render its child as-is.
static bool FilterToSQL(const TableFilter &filter, const string &col, string &out) {
    switch (filter.filter_type) {
    case TableFilterType::CONSTANT_COMPARISON: {
        auto &cf = filter.Cast<ConstantFilter>();
        out = col + ExpressionTypeToOperator(cf.comparison_type) + cf.constant.ToSQLString();
        return true;
    }
    case TableFilterType::IS_NULL:
        out = col + " IS NULL";
        return true;
    case TableFilterType::IS_NOT_NULL:
        out = col + " IS NOT NULL";
        return true;
    case TableFilterType::IN_FILTER: {
        auto          &inf    = filter.Cast<InFilter>();
        string         result = col + " IN (";
        vector<string> vals;
        for (auto &v : inf.values) {
            vals.push_back(v.ToSQLString());
        }
        sort(vals.begin(), vals.end());
        for (idx_t i = 0; i < vals.size(); i++) {
            if (i > 0)
                result += ", ";
            result += vals[i];
        }
        out = result + ")";
        return true;
    }
    case TableFilterType::CONJUNCTION_AND:
    case TableFilterType::CONJUNCTION_OR: {
        bool  is_or = filter.filter_type == TableFilterType::CONJUNCTION_OR;
        auto &conj =
            is_or ? filter.Cast<ConjunctionOrFilter>()
                  : static_cast<const ConjunctionFilter &>(filter.Cast<ConjunctionAndFilter>());
        vector<string> parts;
        for (auto &child : conj.child_filters) {
            string s;
            if (!FilterToSQL(*child, col, s))
                return false;
            if (!s.empty())
                parts.push_back(std::move(s));
        }
        if (parts.empty())
            return false;
        sort(parts.begin(), parts.end());
        if (parts.size() == 1) {
            out = parts[0];
            return true;
        }
        string result = "(";
        for (idx_t i = 0; i < parts.size(); i++) {
            if (i > 0)
                result += is_or ? " OR " : " AND ";
            result += parts[i];
        }
        out = result + ")";
        return true;
    }
    case TableFilterType::EXPRESSION_FILTER: {
        out = filter.Cast<ExpressionFilter>().ToString(col);
        return true;
    }
    case TableFilterType::OPTIONAL_FILTER: {
        auto &opt = filter.Cast<OptionalFilter>();
        if (opt.child_filter) {
            FilterToSQL(*opt.child_filter, col, out);
        }
        return true;
    }
    case TableFilterType::DYNAMIC_FILTER:
        return false;
    default:
        Printer::Print("ViewRewriter: WARNING: unsupported filter type " +
                       std::to_string((uint8_t)filter.filter_type) +
                       " on column '" + col + "' — skipping materialization");
        return false;
    }
}

static bool CollectPushedDownFilters(vector<string> &column_names, TableFilterSet &filters,
                                     vector<string> &fps, bool ignore_optional = false) {
    for (auto &entry : filters.filters) {
        if (ignore_optional && entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
            continue;
        }
        string sql;
        if (!FilterToSQL(*entry.second, column_names[entry.first], sql)) {
            return false;
        }
        fps.push_back(std::move(sql));
    }
    return true;
}

// Try to apply a rewrite rule or enqueue auto-materialization for a GET node.
// `filter_fps` and `where_sql` cover all filters (pushed-down + any LogicalFilter expressions).
// `filter_projection_map` is the LogicalFilter::projection_map, if any, which selects a subset
// of the GET's output columns for the parent. When the filter is dropped we fold it into the
// new GET's projection_ids so the parent sees the same column bindings.
// On a successful rule match, `op` is replaced and the function returns true.
static bool TryRewriteGet(ClientContext &context, unique_ptr<LogicalOperator> &op, LogicalGet &get,
                          const string &table_name, vector<string> filter_fps,
                          ViewRewriterInfo *info, const vector<idx_t> &filter_projection_map) {
    sort(filter_fps.begin(), filter_fps.end());
    string combined_fp;
    for (auto &fp : filter_fps) {
        if (!combined_fp.empty())
            combined_fp += " AND ";
        combined_fp += fp;
    }

    if (combined_fp.empty()) {
        return false;
    }

    if (info->verbose) {
        Printer::Print("ViewRewriter: Examining GET on table '" + table_name + "'");
        Printer::Print("  Fingerprint: " + combined_fp);
    }

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
            if (!get.GetTable()) {
                continue;
            }
            auto  &catalog     = get.GetTable()->catalog;
            string schema_name = get.GetTable()->GetInfo()->schema;

            auto replacement_entry =
                catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema_name,
                                 rule.replacement_view, OnEntryNotFound::RETURN_NULL);

            if (!replacement_entry) {
                if (info->verbose) {
                    Printer::Print("  WARNING: Replacement '" + rule.replacement_view +
                                   "' not found, skipping.");
                }
                continue;
            }

            auto                    &table_entry = replacement_entry->Cast<DuckTableEntry>();
            unique_ptr<FunctionData> bind_data;
            auto table_function = table_entry.GetScanFunction(context, bind_data);

            auto new_get =
                make_uniq<LogicalGet>(get.table_index, table_function, std::move(bind_data),
                                      get.returned_types, get.names, get.virtual_columns);

            // Fold the filter's projection_map into projection_ids.
            // The filter selects filter_projection_map[i]-th entry from the GET's output.
            // GET output is: projection_ids[j] for each j (or j itself if projection_ids empty).
            if (!filter_projection_map.empty()) {
                auto         &base_proj = get.projection_ids;
                vector<idx_t> folded;
                folded.reserve(filter_projection_map.size());
                for (auto pm_idx : filter_projection_map) {
                    folded.push_back(base_proj.empty() ? pm_idx : base_proj[pm_idx]);
                }
                new_get->projection_ids = std::move(folded);
            } else {
                new_get->projection_ids = get.projection_ids;
            }

            auto column_ids = get.GetColumnIds();
            new_get->SetColumnIds(std::move(column_ids));

            op = std::move(new_get);
            info->rewrites_applied++;

            if (info->verbose) {
                Printer::Print("  Rewrite applied: replaced scan of '" + table_name +
                               "' with scan of view '" + rule.replacement_view + "'");
            }
            return true;

        } catch (std::exception &e) {
            if (info->verbose) {
                Printer::Print("  ERROR during rewrite: " + string(e.what()));
            }
        } catch (...) {
        }
    }

    if (!info->auto_materialize) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lk(info->pending_mutex);
        if (info->seen_fingerprints.count(combined_fp)) {
            return false;
        }

        PendingMaterialization pm;
        pm.rule.source_table       = table_name;
        pm.rule.filter_fingerprint = combined_fp;
        pm.rule.replacement_view =
            table_name + "_auto_" +
            std::to_string(info->seen_fingerprints.size());
        pm.where_sql = combined_fp;

        info->seen_fingerprints.insert(combined_fp);
        info->pending.push_back(std::move(pm));
    }

    context.registered_state->GetOrCreate<AutoMaterializeState>("view_rewriter_auto_mat", *info);
    return false;
}

static void RewritePlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                        ViewRewriterInfo *info) {
    // Pattern 1: LOGICAL_FILTER directly above a LOGICAL_GET.
    // Handles column-vs-column predicates that DuckDB cannot push into the scan.
    // Checked BEFORE recursing so we don't also fire Pattern 1 on the same GET.
    if (op->type == LogicalOperatorType::LOGICAL_FILTER && op->children.size() == 1 &&
        op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {

        auto &filter_op = op->Cast<LogicalFilter>();
        auto &get       = op->children[0]->Cast<LogicalGet>();

        string table_name;
        try {
            table_name = GetTableName(get);
        } catch (...) {
            return;
        }
        if (table_name.empty()) {
            return;
        }

        // Collect fingerprints: pushed-down filters first, then LogicalFilter expressions
        vector<string> filter_fps;
        if (!CollectPushedDownFilters(get.names, get.table_filters, filter_fps, true)) {
            if (info->verbose) {
                Printer::Print("ViewRewriter: skipping '" + table_name +
                               "' — pushed-down filter cannot be expressed as SQL");
            }
            return;
        }
        for (auto &expr : filter_op.expressions) {
            filter_fps.push_back(ExpressionToSQL(*expr));
        }
        if (filter_fps.empty()) {
            return;
        }

        // On a match, op is replaced with the new GET (the FILTER node is dropped).
        TryRewriteGet(context, op, get, table_name, std::move(filter_fps), info,
                      filter_op.projection_map);
        return;
    }

    // Recurse into children
    for (auto &child : op->children) {
        RewritePlan(context, child, info);
    }

    // Pattern 2: Bare LOGICAL_GET with pushed-down filters
    if (op->type == LogicalOperatorType::LOGICAL_GET) {
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

        vector<string> filter_fps;
        if (!CollectPushedDownFilters(get.names, get.table_filters, filter_fps)) {
            if (info->verbose) {
                Printer::Print("ViewRewriter: skipping '" + table_name +
                               "' — pushed-down filter cannot be expressed as SQL");
            }
            return;
        }
        if (filter_fps.empty()) {
            return;
        }

        TryRewriteGet(context, op, get, table_name, std::move(filter_fps), info, {});
        return;
    }
}

// ──────────────────────────────────────────────────────────────────────
// The optimizer hook function called after DuckDB's built-in optimizers
// ──────────────────────────────────────────────────────────────────────
static void ViewRewriterOptimize(OptimizerExtensionInput     &input,
                                 unique_ptr<LogicalOperator> &plan) {
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

static unique_ptr<FunctionData> AddRuleBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types,
                                            vector<string>      &names) {
    if (input.inputs.size() != 3) {
        throw BinderException("tpch_rewriter_add_rule requires 3 arguments: "
                              "source_table, filter_fingerprint, replacement_view");
    }

    auto source_table       = input.inputs[0].GetValue<string>();
    auto filter_fingerprint = input.inputs[1].GetValue<string>();
    auto replacement_view   = input.inputs[2].GetValue<string>();

    // Find our optimizer extension info
    auto &config = DBConfig::GetConfig(context);
    for (auto &ext : config.optimizer_extensions) {
        auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
        if (info) {
            RewriteRule rule;
            rule.source_table       = source_table;
            rule.filter_fingerprint = filter_fingerprint;
            rule.replacement_view   = replacement_view;
            info->rules.push_back(std::move(rule));
            break;
        }
    }

    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("status");

    auto bind_data     = make_uniq<AddRuleBindData>();
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

static unique_ptr<FunctionData> StatsBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types,
                                          vector<string>      &names) {
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

    idx_t num_rules        = 0;
    idx_t rewrites_applied = 0;
    auto &config           = DBConfig::GetConfig(context);
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
    info->db  = instance.shared_from_this();

    OptimizerExtension opt_ext;
    opt_ext.optimize_function = ViewRewriterOptimize;
    opt_ext.optimizer_info    = std::move(info);
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

    // SET view_rewriter_verbose = true/false
    config.AddExtensionOption(
        "view_rewriter_verbose",
        "Print verbose optimizer and materialization diagnostics for the view rewriter",
        LogicalType::BOOLEAN, Value::BOOLEAN(false),
        [](ClientContext &context, SetScope scope, Value &value) {
            auto &cfg = DBConfig::GetConfig(context);
            for (auto &ext : cfg.optimizer_extensions) {
                auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
                if (info) {
                    info->verbose = BooleanValue::Get(value);
                    break;
                }
            }
        });

    // SET view_rewriter_min_selectivity = 0.0..1.0
    config.AddExtensionOption(
        "view_rewriter_min_selectivity",
        "Minimum selectivity (fraction of rows filtered out) required to materialize a view. "
        "Views with selectivity below this threshold are skipped.",
        LogicalType::DOUBLE, Value::DOUBLE(0.0),
        [](ClientContext &context, SetScope scope, Value &value) {
            auto &cfg = DBConfig::GetConfig(context);
            for (auto &ext : cfg.optimizer_extensions) {
                auto *info = dynamic_cast<ViewRewriterInfo *>(ext.optimizer_info.get());
                if (info) {
                    info->min_selectivity = value.GetValue<double>();
                    break;
                }
            }
        });

    // SET view_rewriter_auto_materialize = true/false
    config.AddExtensionOption("view_rewriter_auto_materialize",
                              "Automatically materialize filter sub-expressions as in-memory "
                              "tables and register rewrite rules",
                              LogicalType::BOOLEAN, Value::BOOLEAN(false),
                              [](ClientContext &context, SetScope scope, Value &value) {
                                  auto &cfg = DBConfig::GetConfig(context);
                                  for (auto &ext : cfg.optimizer_extensions) {
                                      auto *info = dynamic_cast<ViewRewriterInfo *>(
                                          ext.optimizer_info.get());
                                      if (info) {
                                          info->auto_materialize = BooleanValue::Get(value);
                                          break;
                                      }
                                  }
                              });
}

void ViewRewriterExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string ViewRewriterExtension::Name() { return "view_rewriter"; }

std::string ViewRewriterExtension::Version() const {
#ifdef EXT_VERSION_VIEW_REWRITER
    return EXT_VERSION_VIEW_REWRITER;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(view_rewriter, loader) { duckdb::LoadInternal(loader); }
}
