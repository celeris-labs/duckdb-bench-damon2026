#include <filesystem>
#include <iostream>

#include <duckdb.hpp>

namespace fs = std::filesystem;

// ============================================================================
// Source types
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
    throw std::runtime_error("Unknown source!");
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
// High-resolution timer
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

std::vector<fs::path> get_queries(Source source) {
    // Determine query directory based on source
    std::string query_dir = "sql/tpch/queries/";
    if (source == Source::PROJECTED) {
        query_dir = "sql/tpch/projected_queries/";
    }
    
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

    return queries;
}

void load_views(duckdb::Connection& con, const std::string& dir_path, const std::string& data_dir) {
    for (const auto& entry : fs::directory_iterator(dir_path)) {
        std::ifstream f(entry.path());
        std::string sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

        // Replace hardcoded data path with configured data_dir
        size_t pos;
        while ((pos = sql.find("data/tpch")) != std::string::npos) {
            sql.replace(pos, 9, data_dir);
        }

        auto r = con.Query(sql);
        if (r->HasError()) {
            throw std::runtime_error("Failed to load view: " + r->GetError());
        }
    }
}

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

        auto r = con.Query(sql);
        if (r->HasError()) {
            throw std::runtime_error("Failed to load table '" + table + "': " + r->GetError());
        }
    }
    
    // Load filtered views if needed
    if (source == Source::FILTERED) {
        load_views(con, "sql/tpch/filtered_views", data_dir);
    }
    
    // Load projected views if needed
    if (source == Source::PROJECTED) {
        load_views(con, "sql/tpch/filtered_views", data_dir);
        load_views(con, "sql/tpch/projected_views", data_dir);
    }
}

// ============================================================================
// Arguments
// ============================================================================

std::vector<uint32_t> parse_thread_arg(const std::string &arg) {
    std::vector<uint32_t> threads;
    
    size_t dash_pos = arg.find('-');
    if (dash_pos != std::string::npos) { // Check if it's a range (contains '-')
        uint32_t start = std::stoi(arg.substr(0, dash_pos));
        uint32_t end = std::stoi(arg.substr(dash_pos + 1));
        
        if (start > end || start < 1) {
            throw std::invalid_argument("Invalid thread range: " + arg);
        }
        
        for (uint32_t i = start; i <= end; ++i) {
            threads.push_back(i);
        }
    } else if (arg.find(',') != std::string::npos) { // Check if it's a comma-separated list
        std::stringstream ss(arg);
        std::string token;
        
        while (std::getline(ss, token, ',')) {
            uint32_t val = std::stoi(token);
            if (val < 1) {
                throw std::invalid_argument("Invalid thread count: " + token);
            }
            threads.push_back(val);
        }
    } else { // Single value
        uint32_t val = std::stoi(arg);
        if (val < 1) {
            throw std::invalid_argument("Invalid thread count: " + arg);
        }
        threads.push_back(val);
    }
    
    return threads;
}
