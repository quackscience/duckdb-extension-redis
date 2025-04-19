#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

class CreateRedisSecretFunctions {
public:
    static void Register(DatabaseInstance &instance);
};

} // namespace duckdb 
