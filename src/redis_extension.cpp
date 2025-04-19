#define DUCKDB_EXTENSION_MAIN

#include "redis_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <boost/asio.hpp>
#include <string>

namespace duckdb {

using boost::asio::ip::tcp;

// Simple Redis protocol formatter
class RedisProtocol {
public:
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
            return response.substr(pos + 2);
        }
        return response;
    }
};

// Redis client using Boost.Asio
class RedisClient {
public:
    RedisClient(const std::string& host, const std::string& port) 
        : io_context_(), socket_(io_context_) {
        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host, port);
        boost::asio::connect(socket_, endpoints);
    }

    std::string get(const std::string& key) {
        std::string request = RedisProtocol::formatGet(key);
        boost::asio::write(socket_, boost::asio::buffer(request));
        
        boost::asio::streambuf response;
        boost::asio::read_until(socket_, response, "\r\n");
        
        std::string result((std::istreambuf_iterator<char>(&response)),
                           std::istreambuf_iterator<char>());
        return RedisProtocol::parseResponse(result);
    }

    std::string set(const std::string& key, const std::string& value) {
        std::string request = RedisProtocol::formatSet(key, value);
        boost::asio::write(socket_, boost::asio::buffer(request));
        
        boost::asio::streambuf response;
        boost::asio::read_until(socket_, response, "\r\n");
        
        std::string result((std::istreambuf_iterator<char>(&response)),
                           std::istreambuf_iterator<char>());
        return RedisProtocol::parseResponse(result);
    }

private:
    boost::asio::io_context io_context_;
    tcp::socket socket_;
};

static void RedisGetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &key_vector = args.data[0];
    auto &host_vector = args.data[1];
    auto &port_vector = args.data[2];

    UnaryExecutor::Execute<string_t, string_t>(
        key_vector, result, args.size(),
        [&](string_t key) {
            try {
                RedisClient client(host_vector.GetValue(0).ToString(),
                                 port_vector.GetValue(0).ToString());
                auto response = client.get(key.GetString());
                return StringVector::AddString(result, response);
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

    BinaryExecutor::Execute<string_t, string_t, string_t>(
        key_vector, value_vector, result, args.size(),
        [&](string_t key, string_t value) {
            try {
                RedisClient client(host_vector.GetValue(0).ToString(),
                                 port_vector.GetValue(0).ToString());
                auto response = client.set(key.GetString(), value.GetString());
                return StringVector::AddString(result, response);
            } catch (std::exception &e) {
                throw InvalidInputException("Redis SET error: %s", e.what());
            }
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register functions
    auto redis_get_func = ScalarFunction(
        "redis_get",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        RedisGetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_get_func);

    auto redis_set_func = ScalarFunction(
        "redis_set",
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::VARCHAR,
        RedisSetFunction
    );
    ExtensionUtil::RegisterFunction(instance, redis_set_func);
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