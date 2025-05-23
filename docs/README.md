<img src="https://github.com/user-attachments/assets/46a5c546-7e9b-42c7-87f4-bc8defe674e0" width=250 />

# DuckDB Redis Client Extension
This extension provides Redis client functionality for DuckDB, allowing you to interact with a Redis server directly from SQL queries.

> Experimental: USE AT YOUR OWN RISK!

## Features
Currently supported Redis operations:
- String operations: `GET`, `SET`
- Hash operations: `HGET`, `HSET`
- List operations: `LPUSH`, `LRANGE`
- Batch operations: `MGET`, `SCAN`


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
CREATE SECRET IF NOT EXISTS redis (
        TYPE redis,
        PROVIDER config,
        host 'localhost',
        port '6379',
        password 'optional_password'
    );

-- Create a Redis cloud connection secret
CREATE SECRET IF NOT EXISTS redis (
        TYPE redis,
        PROVIDER config,
        host 'redis-1234.ec2.redns.redis-cloud.com',
        port '16959',
        password 'xxxxxx'
    );
```

### String Operations
```sql
-- Set a value
SELECT redis_set('user:1', 'John Doe', 'redis') as result;

-- Get a value
SELECT redis_get('user:1', 'redis') as user_name;

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
SELECT redis_hset('user:1', 'email', 'john@example.com', 'redis');
SELECT redis_hset('user:1', 'age', '30', 'redis');

-- Get hash field
SELECT redis_hget('user:1', 'email', 'redis') as email;

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
    'redis'
)
FROM profile;
```

### List Operations
```sql
-- Push items to list
SELECT redis_lpush('mylist', 'first_item', 'redis');
SELECT redis_lpush('mylist', 'second_item', 'redis');

-- Get range from list (returns comma-separated values)
-- Get all items (0 to -1 means start to end)
SELECT redis_lrange('mylist', 0, -1, 'redis') as items;

-- Get first 5 items
SELECT redis_lrange('mylist', 0, 4, 'redis') as items;

-- Push multiple items
WITH items(value) AS (
    VALUES ('item1'), ('item2'), ('item3')
)
SELECT redis_lpush('mylist', value, 'redis')
FROM items;
```

### Batch Operations
```sql
-- Get multiple keys at once
SELECT redis_mget('key1,key2,key3', 'redis') as values;
-- Returns comma-separated values for all keys

-- Scan keys matching a pattern
SELECT redis_scan('0', 'user:*', 10, 'redis') as result;
-- Returns: "cursor:key1,key2,key3" where cursor is the next position for scanning
-- Use the returned cursor for the next scan until cursor is 0

-- Scan all keys matching a pattern
WITH RECURSIVE scan(cursor, keys) AS (
    -- Initial scan
    SELECT split_part(redis_scan('0', 'user:*', 10, 'redis'), ':', 1),
           split_part(redis_scan('0', 'user:*', 10, 'redis'), ':', 2)
    UNION ALL
    -- Continue scanning until cursor is 0
    SELECT split_part(redis_scan(cursor, 'user:*', 10, 'redis'), ':', 1),
           split_part(redis_scan(cursor, 'user:*', 10, 'redis'), ':', 2)
    FROM scan
    WHERE cursor != '0'
)
SELECT keys FROM scan;
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

