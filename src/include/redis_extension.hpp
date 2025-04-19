#pragma once

#include "duckdb.hpp"
#include "redis_secret.hpp"

namespace duckdb {

class RedisExtension : public Extension {
public:
    void Load(DuckDB &db) override;
    std::string Name() override;
};

} // namespace duckdb 
