#include <chrono>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <getopt.h>

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir = "/mnt/ramdisk";
    Source source = Source::PARQUET;
    int threads = 1;
    int repetitions = 5;
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
    mean_stats.memory_footprint = std::accumulate(stats.memory_footprint.begin(), stats.memory_footprint.end(), 0ULL) / stats.n;
    mean_stats.rows_scanned     = std::accumulate(stats.rows_scanned.begin(), stats.rows_scanned.end(), 0ULL) / stats.n;
    
    return mean_stats;
}

// ============================================================================
// Benchmark execution
// ============================================================================

struct QueryResult {
    std::string filename;
    MeanStatistics stats;
};

std::vector<QueryResult> run_benchmark(const Config& config, const std::vector<fs::path> &query_paths) {
    std::vector<QueryResult> results;
    
    // Initialize DuckDB
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    duckdb::DuckDB db(nullptr, &duck_config); // In-memory database
    duckdb::Connection con(db);
    
    con.Query("SET enable_object_cache TO false");

    // Load view rewriter extension
    con.Query("LOAD 'extension/build/release/extension/view_rewriter/view_rewriter.duckdb_extension'");
    
    // Load data
    load_data(con, config.data_dir, config.source);

    con.Query("SET threads TO " + std::to_string(config.threads));

    // Enable profiling
    con.Query("PRAGMA enable_profiling='no_output'");
    
    Timer timer;
    
    // Run each query
    for (const auto& query_path : query_paths) {
        Statistics stats;

        // Print filename (matching Python behavior)
        std::cout << query_path.string() << std::endl;
        
        // Read query
        std::ifstream f(query_path);
        std::string sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        
        QueryResult result;
        result.filename = query_path.string();
        
        // Timed runs
        for (int i = 0; i < config.repetitions; ++i) {
            timer.start();
            auto r = con.Query(sql);
            
            if (r->HasError()) {
                throw std::runtime_error("Query error: " + r->GetError());
            }
            
            // Materialize results
            r->Fetch();
            double elapsed = timer.stop_seconds();
            
            // Get statistics
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
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    Config config = parse_args(argc, argv);

    // Get queries based on source
    std::vector<fs::path> query_paths = get_queries(config.source);

    auto results = run_benchmark(config, query_paths);
    
    write_json(config, results, "measurements/queries/" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json");
    
    return 0;
}
