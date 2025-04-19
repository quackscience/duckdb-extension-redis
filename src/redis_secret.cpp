#include "redis_secret.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static void CopySecret(const std::string &key, const CreateSecretInput &input, KeyValueSecret &result) {
    auto val = input.options.find(key);
    if (val != input.options.end()) {
        result.secret_map[key] = val->second;
    }
}

static void RegisterCommonSecretParameters(CreateSecretFunction &function) {
    // Register redis connection parameters
    function.named_parameters["host"] = LogicalType::VARCHAR;
    function.named_parameters["port"] = LogicalType::VARCHAR;
    function.named_parameters["password"] = LogicalType::VARCHAR;
}

static void RedactCommonKeys(KeyValueSecret &result) {
    // Redact sensitive information
    result.redact_keys.insert("password");
}

static unique_ptr<KeyValueSecret> CreateRedisSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
    auto scope = input.scope;
    auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

    // Copy all relevant secrets
    CopySecret("host", input, *result);
    CopySecret("port", input, *result);
    CopySecret("password", input, *result);

    // Redact sensitive keys
    RedactCommonKeys(*result);

    return std::move(result);
}

void CreateRedisSecretFunctions::Register(DatabaseInstance &instance) {
    string type = "redis";

    // Register the new type
    SecretType secret_type;
    secret_type.name = type;
    secret_type.deserializer = KeyValueSecret::Deserialize;
    secret_type.default_provider = "config";
    ExtensionUtil::RegisterSecretType(instance, secret_type);

    // Register the config secret provider
    CreateSecretFunction config_function = {type, "config", CreateRedisSecretFromConfig};
    RegisterCommonSecretParameters(config_function);
    ExtensionUtil::RegisterFunction(instance, config_function);
}

} // namespace duckdb 
