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
        std::string cmd = "*6\r\n$4\r\nSCAN\r\n";
        cmd += "$" + std::to_string(cursor.length()) + "\r\n" + cursor + "\r\n";
        cmd += "$5\r\nMATCH\r\n";
        cmd += "$" + std::to_string(pattern.length()) + "\r\n" + pattern + "\r\n";
        cmd += "$5\r\nCOUNT\r\n";
        auto count_str = std::to_string(count);
        cmd += "$" + std::to_string(count_str.length()) + "\r\n" + count_str + "\r\n";
        return cmd;
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

    static std::string formatMGet(const std::vector<std::string>& keys) {
        std::string cmd = "*" + std::to_string(keys.size() + 1) + "\r\n$4\r\nMGET\r\n";
        for (const auto& key : keys) {
            cmd += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
        }
        return cmd;
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

// Add this helper function
static bool GetRedisSecret(ClientContext &context, const string &secret_name, string &host, string &port, string &password) {
    auto &secret_manager = SecretManager::Get(context);
    try {
        auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
        auto secret_match = secret_manager.LookupSecret(transaction, "redis", secret_name);
        if (secret_match.HasMatch()) {
            auto &secret = secret_match.GetSecret();
            if (secret.GetType() != "redis") {
                throw InvalidInputException("Invalid secret type. Expected 'redis', got '%s'", secret.GetType());
            }
            const auto *kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
            if (!kv_secret) {
                throw InvalidInputException("Invalid secret format for 'redis' secret");
            }
            
            Value host_val, port_val, password_val;
            if (!kv_secret->TryGetValue("host", host_val) || 
                !kv_secret->TryGetValue("port", port_val) ||
                !kv_secret->TryGetValue("password", password_val)) {
                return false;
            }
            
            host = host_val.ToString();
            port = port_val.ToString();
            password = password_val.ToString();
            return true;
        }
    } catch (...) {
        return false;
    }
    return false;
}

// Modify the function signatures to accept secret name instead of connection details
static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &secret_vector = args.data[1];

    UnaryExecutor::Execute<string_t, string_t>(
        key_vector, result, args.size(),
        [&](string_t key) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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
    auto &secret_vector = args.data[2];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, value_vector, result, args.size(),
        [&](string_t key, string_t value) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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
    auto &secret_vector = args.data[2];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, field_vector, result, args.size(),
        [&](string_t key, string_t field) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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
    auto &secret_vector = args.data[3];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, field_vector, result, args.size(),
        [&](string_t key, string_t field) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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
    auto &secret_vector = args.data[2];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, value_vector, result, args.size(),
        [&](string_t key, string_t value) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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
    auto &secret_vector = args.data[3];

    BinaryExecutor::Execute<string_t, int64_t, string_t>(
        key_vector, start_vector, result, args.size(),
        [&](string_t key, int64_t start) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
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

static void RedisMGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &keys_list = args.data[0];
    auto &secret_vector = args.data[1];

    UnaryExecutor::Execute<string_t, string_t>(
        keys_list, result, args.size(),
        [&](string_t keys_str) {
            try {
                // Split comma-separated keys
                std::vector<std::string> keys;
                std::string key_list = keys_str.GetString();
                size_t pos = 0;
                while ((pos = key_list.find(',')) != std::string::npos) {
                    keys.push_back(key_list.substr(0, pos));
                    key_list.erase(0, pos + 1);
                }
                if (!key_list.empty()) {
                    keys.push_back(key_list);
                }

                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
                auto response = conn->execute(RedisProtocol::formatMGet(keys));
                auto values = RedisProtocol::parseArrayResponse(response);
                
                // Join results with comma
                std::string joined;
                for (size_t i = 0; i < values.size(); i++) {
                    if (i > 0) joined += ",";
                    joined += values[i];
                }
                return StringVector::AddString(result, joined);
            } catch (std::exception &e) {
                throw InvalidInputException("Redis MGET error: %s", e.what());
            }
        });
}

static void RedisScanFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &cursor_vector = args.data[0];
    auto &pattern_vector = args.data[1];
    auto &count_vector = args.data[2];
    auto &secret_vector = args.data[3];

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        cursor_vector, pattern_vector, result, args.size(),
        [&](string_t cursor, string_t pattern) {
            try {
                string host, port, password;
                if (!GetRedisSecret(state.GetContext(), secret_vector.GetValue(0).ToString(), 
                                  host, port, password)) {
                    throw InvalidInputException("Redis secret not found");
                }
                
                auto count = count_vector.GetValue(0).GetValue<int64_t>();
                auto conn = ConnectionPool::getInstance().getConnection(host, port, password);
                auto response = conn->execute(RedisProtocol::formatScan(
                    cursor.GetString(), 
                    pattern.GetString(),
                    count
                ));
                auto scan_result = RedisProtocol::parseArrayResponse(response);
                
                if (scan_result.size() >= 2) {
                    // First element is the new cursor, second element is array of keys
                    std::string result_str = scan_result[0] + ":";
                    auto keys = RedisProtocol::parseArrayResponse(scan_result[1]);
                    for (size_t i = 0; i < keys.size(); i++) {
                        if (i > 0) result_str += ",";
                        result_str += keys[i];
                    }
                    return StringVector::AddString(result, result_str);
                }
                return StringVector::AddString(result, "0:");
            } catch (std::exception &e) {
                throw InvalidInputException("Redis SCAN error: %s", e.what());
            }
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register the secret functions first!
    CreateRedisSecretFunctions::Register(instance);

    // Then register Redis functions
    auto redis_get_func = ScalarFunction(
        "redis_get",
        {LogicalType::VARCHAR,    // key
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_get_func);

    // Register Redis SET function
    auto redis_set_func = ScalarFunction(
        "redis_set",
        {LogicalType::VARCHAR,    // key
         LogicalType::VARCHAR,    // value
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisSetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_set_func);

    // Register HGET
    auto redis_hget_func = ScalarFunction(
        "redis_hget",
        {LogicalType::VARCHAR,    // key
         LogicalType::VARCHAR,    // field
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisHGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_hget_func);

    // Register HSET
    auto redis_hset_func = ScalarFunction(
        "redis_hset",
        {LogicalType::VARCHAR,    // key
         LogicalType::VARCHAR,    // field
         LogicalType::VARCHAR,    // value
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisHSetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_hset_func);

    // Register LPUSH
    auto redis_lpush_func = ScalarFunction(
        "redis_lpush",
        {LogicalType::VARCHAR,    // key
         LogicalType::VARCHAR,    // value
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisLPushFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_lpush_func);

    // Register LRANGE
    auto redis_lrange_func = ScalarFunction(
        "redis_lrange",
        {LogicalType::VARCHAR,    // key
         LogicalType::BIGINT,     // start
         LogicalType::BIGINT,     // stop
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisLRangeFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_lrange_func);

    // Register MGET
    auto redis_mget_func = ScalarFunction(
        "redis_mget",
        {LogicalType::VARCHAR,    // comma-separated keys
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisMGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_mget_func);

    // Register SCAN
    auto redis_scan_func = ScalarFunction(
        "redis_scan",
        {LogicalType::VARCHAR,    // cursor
         LogicalType::VARCHAR,    // pattern
         LogicalType::BIGINT,     // count
         LogicalType::VARCHAR},   // secret_name
        LogicalType::VARCHAR,
        RedisScanFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_scan_func);
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
