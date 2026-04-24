#include <filesystem>
#include <iostream>

#include <duckdb.hpp>

namespace fs = std::filesystem;

// ============================================================================
// Benchmark types
// ============================================================================

enum class Benchmark { TPCH = 0, TPCDS = 1, CLICKBENCH = 2 };

const char *benchmark_to_string(Benchmark b) {
    switch (b) {
    case Benchmark::TPCH:
        return "tpch";
    case Benchmark::TPCDS:
        return "tpcds";
    case Benchmark::CLICKBENCH:
        return "clickbench";
    }
    throw std::runtime_error("Unknown benchmark!");
}

Benchmark string_to_benchmark(const std::string &s) {
    if (s == "tpch")
        return Benchmark::TPCH;
    if (s == "tpcds")
        return Benchmark::TPCDS;
    if (s == "clickbench")
        return Benchmark::CLICKBENCH;
    throw std::runtime_error("Unknown benchmark: " + s);
}

// ============================================================================
// Source types
// ============================================================================

enum class Source { PARQUET = 0, CSV = 1, JSON = 2, MEMORY = 3, FILTERED = 4, PROJECTED = 5 };

const char *source_to_string(Source s) {
    switch (s) {
    case Source::PARQUET:
        return "parquet";
    case Source::CSV:
        return "csv";
    case Source::JSON:
        return "json";
    case Source::MEMORY:
        return "memory";
    case Source::FILTERED:
        return "filtered";
    case Source::PROJECTED:
        return "projected";
    }
    throw std::runtime_error("Unknown source!");
}

Source string_to_source(const std::string &s) {
    if (s == "parquet")
        return Source::PARQUET;
    if (s == "csv")
        return Source::CSV;
    if (s == "json")
        return Source::JSON;
    if (s == "memory")
        return Source::MEMORY;
    if (s == "filtered")
        return Source::FILTERED;
    if (s == "projected")
        return Source::PROJECTED;
    throw std::runtime_error("Unknown source: " + s);
}

// ============================================================================
// High-resolution timer
// ============================================================================

class Timer {
  public:
    void start() { start_time_ = std::chrono::high_resolution_clock::now(); }

    double stop_seconds() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time_).count();
        return duration / 1e9;
    }

  private:
    std::chrono::high_resolution_clock::time_point start_time_;
};

// ============================================================================
// Data Loading
// ============================================================================

std::vector<fs::path> get_queries(Benchmark benchmark, Source source) {
    std::string query_dir;
    switch (benchmark) {
    case Benchmark::TPCH:
        query_dir =
            (source == Source::PROJECTED) ? "sql/tpch/projected_queries/" : "sql/tpch/queries/";
        break;
    case Benchmark::TPCDS:
        query_dir = "sql/tpcds/";
        break;
    case Benchmark::CLICKBENCH:
        query_dir = "sql/clickbench/";
        break;
    }

    std::vector<fs::path> queries;
    if (fs::exists(query_dir)) {
        for (const auto &entry : fs::directory_iterator(query_dir)) {
            if (entry.path().extension() == ".sql") {
                queries.push_back(entry.path());
            }
        }
    }
    std::sort(queries.begin(), queries.end());

    return queries;
}

void load_views(duckdb::Connection &con, const std::string &dir_path, const std::string &data_dir) {
    for (const auto &entry : fs::directory_iterator(dir_path)) {
        std::ifstream f(entry.path());
        std::string   sql((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());

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

void load_data(duckdb::Connection &con, const std::string &data_dir, Benchmark benchmark,
               Source source) {
    if (!fs::exists(data_dir) || !fs::is_directory(data_dir)) {
        throw std::runtime_error("Data directory does not exist: " + data_dir);
    }

    // Discover all files in data_dir with the expected extension for this source
    std::string extension;
    switch (source) {
    case Source::CSV:
        extension = ".csv";
        break;
    case Source::JSON:
        extension = ".json";
        break;
    default:
        extension = ".parquet";
        break; // PARQUET, MEMORY, FILTERED, PROJECTED
    }

    std::vector<std::string> tables;
    for (const auto &entry : fs::directory_iterator(data_dir)) {
        if (entry.path().extension() == extension) {
            tables.push_back(entry.path().stem().string());
        }
    }
    std::sort(tables.begin(), tables.end());

    if (tables.size() == 0) {
        throw std::runtime_error("No input data in: " + data_dir);
    }

    std::string replace_cols =
        (benchmark == Benchmark::CLICKBENCH)
            ? " REPLACE(make_date(EventDate) AS EventDate, epoch_ms(EventTime * 1000) AS "
              "EventTime, epoch_ms(ClientEventTime * 1000) AS ClientEventTime, "
              "epoch_ms(LocalEventTime * 1000) AS LocalEventTime)"
            : "";

    for (const auto &table : tables) {
        std::string path = data_dir + "/" + table + extension;
        std::string sql;
        switch (source) {
        case Source::PARQUET:
            sql = "CREATE VIEW " + table + " AS SELECT *" + replace_cols + " FROM read_parquet('" +
                  path + "')";
            break;
        case Source::CSV:
            sql = "CREATE VIEW " + table + " AS SELECT *" + replace_cols + " FROM read_csv('" +
                  path + "')";
            break;
        case Source::JSON:
            sql = "CREATE VIEW " + table + " AS SELECT *" + replace_cols + " FROM read_json('" +
                  path + "')";
            break;
        default: // MEMORY, FILTERED, PROJECTED all load into tables
            sql = "CREATE TABLE " + table + " AS SELECT *" + replace_cols + " FROM read_parquet('" +
                  path + "')";
            break;
        }

        auto r = con.Query(sql);
        if (r->HasError()) {
            throw std::runtime_error("Failed to load table '" + table + "': " + r->GetError());
        }
    }

    // TPC-H specific views
    if (benchmark == Benchmark::TPCH) {
        if (source == Source::FILTERED) {
            load_views(con, "sql/tpch/filtered_views", data_dir);
        }
        if (source == Source::PROJECTED) {
            load_views(con, "sql/tpch/filtered_views", data_dir);
            load_views(con, "sql/tpch/projected_views", data_dir);
        }
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
        uint32_t end   = std::stoi(arg.substr(dash_pos + 1));

        if (start > end || start < 1) {
            throw std::invalid_argument("Invalid thread range: " + arg);
        }

        for (uint32_t i = start; i <= end; ++i) {
            threads.push_back(i);
        }
    } else if (arg.find(',') != std::string::npos) { // Check if it's a comma-separated list
        std::stringstream ss(arg);
        std::string       token;

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
