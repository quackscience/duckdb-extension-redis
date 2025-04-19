#define DUCKDB_EXTENSION_MAIN

#include "redis_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <boost/asio.hpp>
#include <string>
#include <mutex>
#include <unordered_map>
#include <memory>

namespace duckdb {

using boost::asio::ip::tcp;

// Simple Redis protocol formatter
class RedisProtocol {
public:
    static std::string formatAuth(const std::string& password) {
        return "*2\r\n$4\r\nAUTH\r\n$" + std::to_string(password.length()) + "\r\n" + password + "\r\n";
    }

    static std::string formatGet(const std::string& key) {
        return "*2\r\n$3\r\nGET\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    }

    static std::string formatSet(const std::string& key, const std::string& value) {
        return "*3\r\n$3\r\nSET\r\n$" + std::to_string(key.length()) + "\r\n" + key + 
               "\r\n$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    }

    static std::string parseResponse(const std::string& response) {
        if (response.empty()) return "";
        if (response[0] == '$') {
            // Bulk string response
            size_t pos = response.find("\r\n");
            if (pos == std::string::npos) return "";
            
            // Skip the length prefix and first \r\n
            pos += 2;
            std::string value = response.substr(pos);
            
            // Remove trailing \r\n if present
            if (value.size() >= 2 && value.substr(value.size() - 2) == "\r\n") {
                value = value.substr(0, value.size() - 2);
            }
            return value;
        } else if (response[0] == '+') {
            // Simple string response
            return response.substr(1, response.find("\r\n") - 1);
        } else if (response[0] == '-') {
            // Error response
            throw InvalidInputException("Redis error: " + response.substr(1));
        }
        return response;
    }

    // Hash operations
    static std::string formatHGet(const std::string& key, const std::string& field) {
        return "*3\r\n$4\r\nHGET\r\n$" + std::to_string(key.length()) + "\r\n" + key + 
               "\r\n$" + std::to_string(field.length()) + "\r\n" + field + "\r\n";
    }

    static std::string formatHSet(const std::string& key, const std::string& field, const std::string& value) {
        return "*4\r\n$4\r\nHSET\r\n$" + std::to_string(key.length()) + "\r\n" + key +
               "\r\n$" + std::to_string(field.length()) + "\r\n" + field +
               "\r\n$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    }

    static std::string formatHGetAll(const std::string& key) {
        return "*2\r\n$7\r\nHGETALL\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
    }

    // List operations
    static std::string formatLPush(const std::string& key, const std::string& value) {
        return "*3\r\n$5\r\nLPUSH\r\n$" + std::to_string(key.length()) + "\r\n" + key +
               "\r\n$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    }

    static std::string formatLRange(const std::string& key, int64_t start, int64_t stop) {
        auto start_str = std::to_string(start);
        auto stop_str = std::to_string(stop);
        return "*4\r\n$6\r\nLRANGE\r\n$" + std::to_string(key.length()) + "\r\n" + key +
               "\r\n$" + std::to_string(start_str.length()) + "\r\n" + start_str +
               "\r\n$" + std::to_string(stop_str.length()) + "\r\n" + stop_str + "\r\n";
    }

    // Key scanning
    static std::string formatScan(const std::string& cursor, const std::string& pattern = "*", int64_t count = 10) {
        auto count_str = std::to_string(count);
        return "*6\r\n$4\r\nSCAN\r\n$" + std::to_string(cursor.length()) + "\r\n" + cursor +
               "\r\n$5\r\nMATCH\r\n$" + std::to_string(pattern.length()) + "\r\n" + pattern +
               "\r\n$5\r\nCOUNT\r\n$" + std::to_string(count_str.length()) + "\r\n" + count_str + "\r\n";
    }

    static std::vector<std::string> parseArrayResponse(const std::string& response) {
        std::vector<std::string> result;
        if (response.empty() || response[0] != '*') return result;
        
        size_t pos = 1;
        size_t end = response.find("\r\n", pos);
        int array_size = std::stoi(response.substr(pos, end - pos));
        pos = end + 2;

        for (int i = 0; i < array_size; i++) {
            if (response[pos] == '$') {
                pos++;
                end = response.find("\r\n", pos);
                int str_len = std::stoi(response.substr(pos, end - pos));
                pos = end + 2;
                if (str_len >= 0) {
                    result.push_back(response.substr(pos, str_len));
                    pos += str_len + 2;
                }
            }
        }
        return result;
    }
};

// Redis connection class
class RedisConnection {
public:
    RedisConnection(const std::string& host, const std::string& port, const std::string& password = "") 
        : io_context_(), socket_(io_context_) {
        try {
            tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(host, port);
            boost::asio::connect(socket_, endpoints);

            if (!password.empty()) {
                std::string auth_cmd = RedisProtocol::formatAuth(password);
                boost::asio::write(socket_, boost::asio::buffer(auth_cmd));
                
                boost::asio::streambuf response;
                boost::asio::read_until(socket_, response, "\r\n");
                
                std::string auth_response((std::istreambuf_iterator<char>(&response)),
                                        std::istreambuf_iterator<char>());
                RedisProtocol::parseResponse(auth_response);
            }
        } catch (std::exception& e) {
            throw InvalidInputException("Redis connection error: " + std::string(e.what()));
        }
    }

    std::string execute(const std::string& command) {
        std::lock_guard<std::mutex> lock(mutex_);
        try {
            boost::asio::write(socket_, boost::asio::buffer(command));
            
            boost::asio::streambuf response;
            boost::asio::read_until(socket_, response, "\r\n");
            
            return std::string((std::istreambuf_iterator<char>(&response)),
                             std::istreambuf_iterator<char>());
        } catch (std::exception& e) {
            throw InvalidInputException("Redis execution error: " + std::string(e.what()));
        }
    }

private:
    boost::asio::io_context io_context_;
    tcp::socket socket_;
    std::mutex mutex_;
};

// Connection pool manager
class ConnectionPool {
public:
    static ConnectionPool& getInstance() {
        static ConnectionPool instance;
        return instance;
    }

    std::shared_ptr<RedisConnection> getConnection(const std::string& host, 
                                                  const std::string& port,
                                                  const std::string& password = "") {
        std::string key = host + ":" + port;
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = connections_.find(key);
        if (it == connections_.end()) {
            auto conn = std::make_shared<RedisConnection>(host, port, password);
            connections_[key] = conn;
            return conn;
        }
        return it->second;
    }

private:
    ConnectionPool() {}
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<RedisConnection>> connections_;
};

static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &host_vector = args.data[1];
    auto &port_vector = args.data[2];
    auto &password_vector = args.data[3];  // New password parameter

    UnaryExecutor::Execute<string_t, string_t>(
        key_vector, result, args.size(),
        [&](string_t key) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto response = conn->execute(RedisProtocol::formatGet(key.GetString()));
                return StringVector::AddString(result, RedisProtocol::parseResponse(response));
            } catch (std::exception &e) {
                throw InvalidInputException("Redis GET error: %s", e.what());
            }
        });
}

static void RedisSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &value_vector = args.data[1];
    auto &host_vector = args.data[2];
    auto &port_vector = args.data[3];
    auto &password_vector = args.data[4];  // New password parameter

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, value_vector, result, args.size(),
        [&](string_t key, string_t value) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto response = conn->execute(RedisProtocol::formatSet(key.GetString(), value.GetString()));
                return StringVector::AddString(result, RedisProtocol::parseResponse(response));
            } catch (std::exception &e) {
                throw InvalidInputException("Redis SET error: %s", e.what());
            }
        });
}

// Hash operations
static void RedisHGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &field_vector = args.data[1];
    auto &host_vector = args.data[2];
    auto &port_vector = args.data[3];
    auto &password_vector = args.data[4];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, field_vector, result, args.size(),
        [&](string_t key, string_t field) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto response = conn->execute(RedisProtocol::formatHGet(key.GetString(), field.GetString()));
                return StringVector::AddString(result, RedisProtocol::parseResponse(response));
            } catch (std::exception &e) {
                throw InvalidInputException("Redis HGET error: %s", e.what());
            }
        });
}

static void RedisHSetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &field_vector = args.data[1];
    auto &value_vector = args.data[2];
    auto &host_vector = args.data[3];
    auto &port_vector = args.data[4];
    auto &password_vector = args.data[5];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, field_vector, result, args.size(),
        [&](string_t key, string_t field) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto response = conn->execute(RedisProtocol::formatHSet(
                    key.GetString(), 
                    field.GetString(),
                    value_vector.GetValue(0).ToString()
                ));
                return StringVector::AddString(result, RedisProtocol::parseResponse(response));
            } catch (std::exception &e) {
                throw InvalidInputException("Redis HSET error: %s", e.what());
            }
        });
}

// List operations
static void RedisLPushFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &value_vector = args.data[1];
    auto &host_vector = args.data[2];
    auto &port_vector = args.data[3];
    auto &password_vector = args.data[4];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, value_vector, result, args.size(),
        [&](string_t key, string_t value) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto response = conn->execute(RedisProtocol::formatLPush(key.GetString(), value.GetString()));
                return StringVector::AddString(result, RedisProtocol::parseResponse(response));
            } catch (std::exception &e) {
                throw InvalidInputException("Redis LPUSH error: %s", e.what());
            }
        });
}

static void RedisLRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &start_vector = args.data[1];
    auto &stop_vector = args.data[2];
    auto &host_vector = args.data[3];
    auto &port_vector = args.data[4];
    auto &password_vector = args.data[5];

    BinaryExecutor::Execute<string_t, int64_t, string_t>(
        key_vector, start_vector, result, args.size(),
        [&](string_t key, int64_t start) {
            try {
                auto conn = ConnectionPool::getInstance().getConnection(
                    host_vector.GetValue(0).ToString(),
                    port_vector.GetValue(0).ToString(),
                    password_vector.GetValue(0).ToString()
                );
                auto stop = stop_vector.GetValue(0).GetValue<int64_t>();
                auto response = conn->execute(RedisProtocol::formatLRange(key.GetString(), start, stop));
                auto values = RedisProtocol::parseArrayResponse(response);
                // Join array values with comma for string result
                std::string joined;
                for (size_t i = 0; i < values.size(); i++) {
                    if (i > 0) joined += ",";
                    joined += values[i];
                }
                return StringVector::AddString(result, joined);
            } catch (std::exception &e) {
                throw InvalidInputException("Redis LRANGE error: %s", e.what());
            }
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register Redis GET function with optional password
    auto redis_get_func = ScalarFunction(
        "redis_get",
        {LogicalType::VARCHAR,                                // key
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_get_func);

    // Register Redis SET function with optional password
    auto redis_set_func = ScalarFunction(
        "redis_set",
        {LogicalType::VARCHAR,                                // key
         LogicalType::VARCHAR,                                // value
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisSetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_set_func);

    // Register HGET
    auto redis_hget_func = ScalarFunction(
        "redis_hget",
        {LogicalType::VARCHAR,                                // key
         LogicalType::VARCHAR,                                // field
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisHGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_hget_func);

    // Register HSET
    auto redis_hset_func = ScalarFunction(
        "redis_hset",
        {LogicalType::VARCHAR,                                // key
         LogicalType::VARCHAR,                                // field
         LogicalType::VARCHAR,                                // value
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisHSetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_hset_func);

    // Register LPUSH
    auto redis_lpush_func = ScalarFunction(
        "redis_lpush",
        {LogicalType::VARCHAR,                                // key
         LogicalType::VARCHAR,                                // value
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisLPushFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_lpush_func);

    // Register LRANGE
    auto redis_lrange_func = ScalarFunction(
        "redis_lrange",
        {LogicalType::VARCHAR,                                // key
         LogicalType::BIGINT,                                 // start
         LogicalType::BIGINT,                                 // stop
         LogicalType::VARCHAR,                                // host
         LogicalType::VARCHAR,                                // port
         LogicalType::VARCHAR},                               // password
        LogicalType::VARCHAR,
        RedisLRangeFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_lrange_func);
}

void RedisExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

std::string RedisExtension::Name() {
    return "redis";
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void redis_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::RedisExtension>();
}

DUCKDB_EXTENSION_API const char *redis_version() {
    return duckdb::DuckDB::LibraryVersion();
}
} 