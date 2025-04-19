<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Redis Client Extension
This extension provides Redis client functionality for DuckDB, allowing you to interact with a Redis server directly from SQL queries.

> Experimental: USE AT YOUR OWN RISK!

## Features
Currently supported Redis operations:
- String operations: `GET`, `SET`
- Hash operations: `HGET`, `HSET`
- List operations: `LPUSH`, `LRANGE`

## Installation
```sql
INSTALL redis FROM community;
LOAD redis;
```

## Usage
### Setting up Redis Connection
First, create a secret to store your Redis connection details:
```sql
-- Create a Redis connection secret
CALL redis_create_secret('my_redis', {
    'host': 'localhost',
    'port': '6379',
    'password': 'optional_password'
});

-- For cloud Redis services (e.g., Redis Labs)
CALL redis_create_secret('redis_cloud', {
    'host': 'redis-xxxxx.cloud.redislabs.com',
    'port': '16379',
    'password': 'your_password'
});
```

### String Operations
```sql
-- Set a value
SELECT redis_set('user:1', 'John Doe', 'my_redis') as result;

-- Get a value
SELECT redis_get('user:1', 'my_redis') as user_name;

-- Set multiple values in a query
INSERT INTO users (id, name)
SELECT id, redis_set(
    'user:' || id::VARCHAR,
    name,
    'my_redis'
)
FROM new_users;
```

### Hash Operations
```sql
-- Set hash fields
SELECT redis_hset('user:1', 'email', 'john@example.com', 'my_redis');
SELECT redis_hset('user:1', 'age', '30', 'my_redis');

-- Get hash field
SELECT redis_hget('user:1', 'email', 'my_redis') as email;

-- Store user profile as hash
WITH profile(id, field, value) AS (
    VALUES 
        (1, 'name', 'John Doe'),
        (1, 'email', 'john@example.com'),
        (1, 'age', '30')
)
SELECT redis_hset(
    'user:' || id::VARCHAR,
    field,
    value,
    'my_redis'
)
FROM profile;
```

### List Operations
```sql
-- Push items to list
SELECT redis_lpush('mylist', 'first_item', 'my_redis');
SELECT redis_lpush('mylist', 'second_item', 'my_redis');

-- Get range from list (returns comma-separated values)
-- Get all items (0 to -1 means start to end)
SELECT redis_lrange('mylist', 0, -1, 'my_redis') as items;

-- Get first 5 items
SELECT redis_lrange('mylist', 0, 4, 'my_redis') as items;

-- Push multiple items
WITH items(value) AS (
    VALUES ('item1'), ('item2'), ('item3')
)
SELECT redis_lpush('mylist', value, 'my_redis')
FROM items;
```

## Error Handling
The extension functions will throw exceptions with descriptive error messages when:
- Redis secret is not found or invalid
- Unable to connect to Redis server
- Network communication errors occur
- Invalid Redis protocol responses are received

## Building from Source
Follow the standard DuckDB extension build process:

```sh
# Install vcpkg dependencies
./vcpkg/vcpkg install boost-asio

# Build the extension
make
```

## Future Enhancements
Planned features include:
- Table functions for scanning Redis keys
- Additional Redis commands (SADD, SMEMBERS, etc.)
- Batch operations using Redis pipelines
- Connection timeout handling

