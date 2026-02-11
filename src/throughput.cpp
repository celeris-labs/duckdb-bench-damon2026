#include <chrono>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <thread>
#include <mutex>
#include <atomic>
#include <getopt.h>

#include <duckdb.hpp>

#include "common.hpp"

// ============================================================================
// Configuration
// ============================================================================

struct Config {
    std::string data_dir = "/mnt/ramdisk";
    Source source = Source::PARQUET;
    std::vector<uint32_t> threads{1};
    uint32_t repetitions = 5;
    uint32_t streams = 2;  // Number of concurrent query streams
};

// ============================================================================
// Query Stream Worker
// ============================================================================

class QueryStream {
private:
    int stream_id_;
    duckdb::Connection con_;
    std::vector<std::string> queries_;

public:
    QueryStream(int stream_id, 
                std::shared_ptr<duckdb::DuckDB> db,
                const std::vector<std::string> &queries)
        : stream_id_(stream_id)
        , con_(*db)
        , queries_(queries)
    {
        std::mt19937 g(stream_id);
        std::shuffle(queries_.begin(), queries_.end(), g);
    }

    void run() {
        for (const auto &query : queries_) {
            auto r = con_.Query(query);
            r->Fetch();
        }
    }
};

// ============================================================================
// Benchmark Execution
// ============================================================================

double run_throughput_benchmark(std::shared_ptr<duckdb::DuckDB> db, uint32_t num_streams, 
        uint32_t num_repetitions, const std::vector<std::string>& queries) {        
    std::vector<QueryStream> streams;
    
    for (int i = 0; i < num_streams; ++i) {
        streams.push_back(QueryStream(i, db, queries));
    }
    
    Timer timer;
    std::vector<double> runtimes;

    for (int i = 0; i < num_repetitions; i++) {
        std::vector<std::thread> threads;

        timer.start();
        
        for (int j = 0; j < num_streams; j++) {
            threads.emplace_back([&streams, j]() {
                streams[j].run();
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        double elapsed = timer.stop_seconds();
        runtimes.push_back(elapsed);
    }
    
    return std::accumulate(runtimes.begin(), runtimes.end(), 0.0) / num_repetitions;
}

// ============================================================================
// Command Line Parsing
// ============================================================================

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "TPC-H throughput benchmark with concurrent query streams.\n"
              << "\n"
              << "Options:\n"
              << "  -s, --source SRC      Input source: parquet, csv, json, memory,\n"
              << "                        filtered, projected (default: parquet)\n"
              << "  -r, --repetitions N   Number of repetitions per query (default: 5)\n"
              << "  -t, --threads N       Number of DuckDB threads as range \"1-8\",\n"
              << "                        list \"1,2,4\", or single value (default: 1)\n"
              << "  -S, --streams N       Number of concurrent query streams (default: 2)\n"
              << "  -d, --data-dir DIR    Data directory (default: /mnt/ramdisk)\n"
              << "  -h, --help            Show this help message\n";
}

Config parse_args(int argc, char* argv[]) {
    Config config;
    
    static struct option long_options[] = {
        {"source",      required_argument, 0, 's'},
        {"repetitions", required_argument, 0, 'r'},
        {"threads",     required_argument, 0, 't'},
        {"streams",     required_argument, 0, 'S'},
        {"data-dir",    required_argument, 0, 'd'},
        {"help",        no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "s:r:t:S:d:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 's':
                config.source = string_to_source(optarg);
                break;
            case 't':
                try {
                    config.threads = parse_thread_arg(optarg);
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing threads arg: " << e.what() << std::endl;
                    print_usage(argv[0]);
                    exit(1);
                }
                break;
            case 'S':
                config.streams = std::stoi(optarg);
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

void write_json(uint32_t num_threads, const Config& config, double result, 
                const std::string& output_file) {
    std::ofstream out(output_file);
    
    // Write result
    out << "{\n";
    out << "    \"runtime_sec\": " << std::fixed << std::setprecision(6) 
        << result << ",\n";
    out << "    \"threads\": " << num_threads << ",\n";
    out << "    \"streams\": " << config.streams << ",\n";
    out << "    \"data_dir\": \"" << config.data_dir << "\",\n";
    out << "    \"source\": \"" << source_to_string(config.source) << "\",\n";
    out << "    \"repetitions\": " << config.repetitions << "\n";
    out << "}\n";
    
    out.close();
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char* argv[]) {
    Config config = parse_args(argc, argv);
    
    // Get queries based on source
    std::vector<fs::path> query_paths = get_queries(config.source);
    std::vector<std::string> queries;
    for (const auto &query_path : query_paths) {
        std::ifstream f(query_path);
        std::string query((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        queries.push_back(query);
    }

    // Initialize DuckDB instance
    duckdb::DBConfig duck_config;
    duck_config.SetOptionByName("allow_unsigned_extensions", duckdb::Value::BOOLEAN(true));
    auto db = std::make_shared<duckdb::DuckDB>(nullptr, &duck_config);
    duckdb::Connection setup_con(*db);
    
    setup_con.Query("SET GLOBAL enable_object_cache TO false");

    // Load view rewriter extension
    setup_con.Query("LOAD 'extension/build/release/extension/view_rewriter/view_rewriter.duckdb_extension'");
    
    // Load data
    std::cout << "Loading data..." << std::endl;
    load_data(setup_con, config.data_dir, config.source);

    // Run benchmark
    for (uint32_t num_threads : config.threads) {
        setup_con.Query("SET GLOBAL threads TO " + std::to_string(num_threads));

        std::cout << "Running throughput benchmark for " << std::to_string(num_threads) << " thread(s)..." << std::endl;
        auto result = run_throughput_benchmark(db, config.streams, config.repetitions, queries);
        
        std::string output_file = "measurements/throughput/" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";
        write_json(num_threads, config, result, output_file);
        
        std::cout << "Results written to: " << output_file << std::endl;
    }
    
    return 0;
}