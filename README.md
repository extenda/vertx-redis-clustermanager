[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=extenda_vertx-redis-clustermanager&metric=alert_status&token=6d4cad0689d8f37a1f02630ddac30099ded3050c)](https://sonarcloud.io/summary/new_code?id=extenda_vertx-redis-clustermanager)

# vertx-redis-clustermanager

A Vert.x ClusterManager for Redis. The cluster manager allows teams to use Redis as a drop-in replacement for Hazelcast
to run Vert.x in clustered high availability mode.

The cluster manager is possible to use with
[Google Cloud MemoryStore for Redis](https://cloud.google.com/memorystore/docs/redis).

## Usage

```xml

<dependency>
  <groupId>com.retailsvc</groupId>
  <artifactId>vertx-redis-clustermanager</artifactId>
  <version>VERSION</version>
</dependency>
```

### Configuration

The manager can be configured programmatically, but the preferred option is to use the supported system properties or
environment variables.

| System Property               | Environment Variable          | Default                                  | Description                                                                                                      |
|-------------------------------|-------------------------------|------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| redis.connection.address      | REDIS_CONNECTION_ADDRESS      | Created from other connection properties | Set the fully qualified Redis address. This is an optional property.                                             |
| redis.connection.host         | REDIS_CONNECTION_HOST         | 127.0.0.1                                | The Redis server hostname or IP address.                                                                         |
| redis.connection.port         | REDIS_CONNECTION_PORT         | 6379                                     | The Redis server port.                                                                                           |
| redis.connection.scheme       | REDIS_CONNECTION_SCHEME       | redis                                    | The Redis scheme. Use <code>redis</code> for TCP and <code>rediss</code> for TLS.                                |
| redis.key.namespace           | REDIS_KEY_NAMESPACE           |                                          | Optional namespace to prefix all keys with. This is useful if the Redis instance is shared by multiple services. |
| redis.use.connection.listener | REDIS_USE_CONNECTION_LISTENER | false                                    | Use a connection listener to automatically reconnect to Redis (EXPERIMENTAL).                                    |

## Development

The project is built with OpenJDK 21 and Maven.

To build the project with all tests, run
```bash
./mnw verify
```

Ensure you install and enable [pre-commit](https://pre-commit.com) before committing code.

```bash
pre-commit install -t pre-commit -t commit-msg
```

When developing your applications, it can be handy to spin up a Redis with Docker.
```bash
docker run --rm -it -p 6379:6379 redis:6-alpine redis-server --save ''
```
