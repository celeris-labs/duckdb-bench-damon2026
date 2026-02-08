/**
 * TPC-H Benchmark Runner for DuckDB (C++)
 * 
 * Equivalent functionality to the Python version with:
 * - Same source types (parquet, csv, json, memory, filtered, projected)
 * - Same command-line interface
 * - Proper high-resolution timing
 * - Statistical analysis (mean, median, stddev, min, max)
 * - Optional CSV output
 * - Optional warmup runs
 */

#include <duckdb.hpp>

#include <chrono>
#include <vector>
#include <string>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <getopt.h>

namespace fs = std::filesystem;

// ============================================================================
// Source Types (matching Python enum)
// ============================================================================

enum class Source {
    PARQUET   = 0,
    CSV       = 1,
    JSON      = 2,
    MEMORY    = 3,
    FILTERED  = 4,
    PROJECTED = 5
};

const char* source_to_string(Source s) {
    switch (s) {
        case Source::PARQUET:   return "parquet";
        case Source::CSV:       return "csv";
        case Source::JSON:      return "json";
        case Source::MEMORY:    return "memory";
        case Source::FILTERED:  return "filtered";
        case Source::PROJECTED: return "projected";
    }
}

Source string_to_source(const std::string& s) {
    if (s == "parquet")   return Source::PARQUET;
    if (s == "csv")       return Source::CSV;
    if (s == "json")      return Source::JSON;
    if (s == "memory")    return Source::MEMORY;
    if (s == "filtered")  return Source::FILTERED;
    if (s == "projected") return Source::PROJECTED;
    throw std::runtime_error("Unknown source: " + s);
}

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir = "data/tpch";
    Source source = Source::PARQUET;
    int threads = 1;
    int repetitions = 5;
    int warmup_runs = 0;
    bool verbose = false;
};

// ============================================================================
// Statistics
// ============================================================================

struct Statistics {
    std::vector<double> runtime;
    std::vector<double> latency;
    std::vector<double> cpu_time;
    std::vector<size_t> memory_footprint;
    std::vector<size_t> rows_scanned;
    size_t n = 0;
};

struct MeanStatistics {
    double runtime;
    double latency;
    double cpu_time;
    size_t memory_footprint;
    size_t rows_scanned;
};

MeanStatistics mean_stats(Statistics &stats) {
    MeanStatistics mean_stats{};
    if (stats.n == 0) return mean_stats;
    
    mean_stats.runtime          = std::accumulate(stats.runtime.begin(), stats.runtime.end(), 0.0) / stats.n;
    mean_stats.latency          = std::accumulate(stats.latency.begin(), stats.latency.end(), 0.0) / stats.n;
    mean_stats.cpu_time         = std::accumulate(stats.cpu_time.begin(), stats.cpu_time.end(), 0.0) / stats.n;
    mean_stats.memory_footprint = std::accumulate(stats.memory_footprint.begin(), stats.memory_footprint.end(), 0) / stats.n;
    mean_stats.rows_scanned     = std::accumulate(stats.rows_scanned.begin(), stats.rows_scanned.end(), 0) / stats.n;
    
    return mean_stats;
}

// ============================================================================
// High-Resolution Timer
// ============================================================================

class Timer {
public:
    void start() {
        start_time_ = std::chrono::high_resolution_clock::now();
    }

    double stop_seconds() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            end_time - start_time_).count();
        return duration / 1e9;
    }

private:
    std::chrono::high_resolution_clock::time_point start_time_;
};

// ============================================================================
// Data Loading
// ============================================================================

void load_data(duckdb::Connection& con, const std::string& data_dir, Source source) {
    const std::vector<std::string> tables = {
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
    };

    if (!fs::exists(data_dir) || !fs::is_directory(data_dir)) {
        throw std::runtime_error("Data directory does not exist: " + data_dir);
    }
    
    for (const auto& table : tables) {
        std::string sql;
        switch (source) {
            case Source::PARQUET:
                sql = "CREATE VIEW " + table + " AS SELECT * FROM read_parquet('" +
                      data_dir + "/" + table + ".parquet')"; break;
            case Source::CSV:
                sql = "CREATE VIEW " + table + " AS SELECT * FROM read_csv('" +
                      data_dir + "/" + table + ".csv')"; break;
            case Source::JSON:
                sql = "CREATE VIEW " + table + " AS SELECT * FROM read_json('" +
                      data_dir + "/" + table + ".json')"; break;
            default: // MEMORY, FILTERED, PROJECTED all load standard tables
                sql = "CREATE TABLE " + table + " AS SELECT * FROM read_parquet('" +
                      data_dir + "/" + table + ".parquet')"; break;
        }
        con.Query(sql);
    }
    
    // Load filtered views if needed
    if (source == Source::FILTERED || source == Source::PROJECTED) {
        for (const auto& entry : fs::directory_iterator("sql/tpch/filtered_views/")) {
            std::ifstream f(entry.path());
            std::string sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
            con.Query(sql);
        }
    }
    
    // Load projected views if needed
    if (source == Source::PROJECTED) {
        for (const auto& entry : fs::directory_iterator("sql/tpch/projected_views/")) {
            std::ifstream f(entry.path());
            std::string sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
            con.Query(sql);
        }
    }
}

// ============================================================================
// Benchmark Execution
// ============================================================================

struct QueryResult {
    std::string filename;
    MeanStatistics stats;
};

std::vector<QueryResult> run_benchmark(const Config& config) {
    std::vector<QueryResult> results;
    
    // Determine query directory based on source
    std::string query_dir = "sql/tpch/queries/";
    if (config.source == Source::FILTERED) {
        query_dir = "sql/tpch/filtered_queries/";
    } else if (config.source == Source::PROJECTED) {
        query_dir = "sql/tpch/projected_queries/";
    }
    
    // Initialize DuckDB
    duckdb::DuckDB db(nullptr); // In-memory database
    duckdb::Connection con(db);
    
    con.Query("SET enable_object_cache TO false");
    
    // Load data
    load_data(con, config.data_dir, config.source);
    
    // Discover and sort query files
    std::vector<fs::path> queries;
    if (fs::exists(query_dir)) {
        for (const auto& entry : fs::directory_iterator(query_dir)) {
            if (entry.path().extension() == ".sql") {
                queries.push_back(entry.path());
            }
        }
    }
    std::sort(queries.begin(), queries.end());

    con.Query("SET threads TO " + std::to_string(config.threads));

    // Enable profiling
    con.Query("PRAGMA enable_profiling='no_output'");
    
    Timer timer;
    
    // Run each query
    for (const auto& query_path : queries) {
        Statistics stats;

        // Print filename (matching Python behavior)
        std::cout << query_path.string() << std::endl;
        
        // Read query
        std::ifstream f(query_path);
        std::string sql((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
        
        QueryResult result;
        result.filename = query_path.string();
        
        // Warmup runs (optional, not in original Python)
        for (int i = 0; i < config.warmup_runs; ++i) {
            auto r = con.Query(sql);
            if (r->HasError()) {
                std::cerr << "Query error: " << r->GetError() << std::endl;
            }
        }
        
        // Timed runs
        for (int i = 0; i < config.repetitions; ++i) {
            timer.start();
            auto r = con.Query(sql);
            
            if (r->HasError()) {
                std::cerr << "Query error: " << r->GetError() << std::endl;
            }
            
            // Materialize results (equivalent to fetch_arrow_table())
            r->Fetch();
            double elapsed = timer.stop_seconds();
            
            auto profiling_info = con.GetProfilingTree()->GetProfilingInfo();
            stats.runtime.push_back(elapsed);
            stats.latency.push_back(profiling_info.GetMetricValue<double>(duckdb::MetricsType::LATENCY));
            stats.cpu_time.push_back(profiling_info.GetMetricValue<double>(duckdb::MetricsType::CPU_TIME));
            stats.rows_scanned.push_back(profiling_info.GetMetricValue<size_t>(duckdb::MetricsType::CUMULATIVE_ROWS_SCANNED));
            stats.memory_footprint.push_back(profiling_info.GetMetricValue<size_t>(duckdb::MetricsType::SYSTEM_PEAK_BUFFER_MEMORY));
            stats.n++;
        }
        
        // Calculate average
        result.stats = mean_stats(stats);
        results.push_back(result);
    }
    
    return results;
}

// ============================================================================
// Write JSON
// ============================================================================

void write_json(const Config &config, const std::vector<QueryResult> &results, const std::string &output_file) {
    std::ofstream out(output_file);
    
    // Write results
    out << "[\n";
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        out << "    {\n";
        out << "        \"query\": \"" << r.filename << "\",\n";
        out << "        \"runtime_sec\": " << std::fixed << std::setprecision(6) 
            << r.stats.runtime << ",\n";
        out << "        \"latency_sec\": " << std::fixed << std::setprecision(6) 
            << r.stats.latency << ",\n";
        out << "        \"cpu_time_sec\": " << std::fixed << std::setprecision(6) 
            << r.stats.cpu_time << ",\n";
        out << "        \"memory_footprint_bytes\": " << r.stats.memory_footprint << ",\n";
        out << "        \"rows_scanned\": " << r.stats.rows_scanned << ",\n";
        out << "        \"data_dir\": \"" << config.data_dir << "\",\n";
        out << "        \"source\": \"" << source_to_string(config.source) << "\",\n";
        out << "        \"threads\": " << config.threads << ",\n";
        out << "        \"repetitions\": " << config.repetitions << "\n";
        out << "    }" << (i < results.size() - 1 ? "," : "") << "\n";
    }
    out << "]\n";

    out.close();
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "DuckDB benchmark with different input configurations.\n"
              << "\n"
              << "Options:\n"
              << "  -s, --source SRC      Input source: parquet, csv, json, memory,\n"
              << "                        filtered, projected (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of threads (default: 1)\n"
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char* argv[]) {
    Config config;
    
    static struct option long_options[] = {
        {"source",      required_argument, 0, 's'},
        {"repetitions", required_argument, 0, 'r'},
        {"threads",     required_argument, 0, 't'},
        {"data-dir",    required_argument, 0, 'd'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "s:r:t:d:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's':
                config.source = string_to_source(optarg);
                break;
            case 'r':
                config.repetitions = std::stoi(optarg);
                break;
            case 't':
                config.threads = std::stoi(optarg);
                break;
            case 'd':
                config.data_dir = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                exit(0);
            default:
                print_usage(argv[0]);
                exit(1);
        }
    }
    
    return config;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    Config config = parse_args(argc, argv);
    
    auto results = run_benchmark(config);
    
    write_json(config, results, "measurements/run-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json");
    
    return 0;
}