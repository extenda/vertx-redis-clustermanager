[![Maven Central Version](https://img.shields.io/maven-central/v/com.retailsvc/vertx-redis-clustermanager?style=flat)](https://central.sonatype.com/artifact/com.retailsvc/vertx-redis-clustermanager)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=extenda_vertx-redis-clustermanager&metric=alert_status&token=6d4cad0689d8f37a1f02630ddac30099ded3050c)](https://sonarcloud.io/summary/new_code?id=extenda_vertx-redis-clustermanager)

# vertx-redis-clustermanager

A Vert.x ClusterManager for Redis. The cluster manager allows teams to use Redis as a drop-in replacement for Hazelcast
to run Vert.x in clustered high availability mode.

The cluster manager is proven in production with [Redis CE][redis-ce]
and [Google Cloud MemoryStore for Redis][memorystore] and can also be used
with [AWS ElastiCache Cluster][elasticache], [Amazon MemoryDB][memorydb] and [Azure Redis Cache][azure].

[redis-ce]: https://redis.io/docs/latest/operate/oss_and_stack/
[memorystore]: https://cloud.google.com/memorystore/docs/redis
[elasticache]: https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/WhatIs.html#WhatIs.Clusters
[memorydb]: https://aws.amazon.com/memorydb/
[azure]: https://azure.microsoft.com/en-us/products/cache/

## Usage

```xml
<dependency>
  <groupId>com.retailsvc</groupId>
  <artifactId>vertx-redis-clustermanager</artifactId>
  <version>VERSION</version>
</dependency>
```

### Configuration

The cluster manager can be configured either programmatically or with environment variables. There's two types of configuration

  * Redis connection settings
  * Data type configuration settings (maps, lock etc)

### Environment variables

System properties or environment variables can be used to configure a standalone client.

| System Property           | Environment Variable      | Default                                  | Description                                                                                                      |
|---------------------------|---------------------------|------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| redis.connection.address  | REDIS_CONNECTION_ADDRESS  | Created from other connection properties | Set the fully qualified Redis address. This is an optional property.                                             |
| redis.connection.scheme   | REDIS_CONNECTION_SCHEME   | redis                                    | The Redis scheme. Use `redis` for TCP and `rediss` for TLS.                                                      |
| redis.connection.host     | REDIS_CONNECTION_HOST     | 127.0.0.1                                | The Redis server hostname or IP address.                                                                         |
| redis.connection.port     | REDIS_CONNECTION_PORT     | 6379                                     | The Redis server port.                                                                                           |
| redis.connection.username | REDIS_CONNECTION_USERNAME |                                          | Optional username to use when connecting to Redis.                                                               |
| redis.connection.password | REDIS_CONNECTION_PASSWORD |                                          | Optional password to use when connecting to Redis.                                                               |
| redis.key.namespace       | REDIS_KEY_NAMESPACE       |                                          | Optional namespace to prefix all keys with. This is useful if the Redis instance is shared by multiple services. |

If you need to configure a client type with multiple node addresses, you'll need to pass configuration to the cluster
manager. The configuration can be loaded from a JSON file or inlined in Java. The above environment variables are
respected from the Java API, but can be overridden.

### JSON configuration

The cluster manager can be configured through JSON files. The file must be loaded and passed as a `RedisConfig`
to the cluster manager constructor. The JSON configuration can be used to configure all properties, but it is not recommended to set any secret or sensitive
information in JSON.

The following properties are supported:

| Property             | Description                                                                                                      | Default Value             |
|----------------------|------------------------------------------------------------------------------------------------------------------|---------------------------|
| clientType           | The Redis client type, one of `STANDALONE`, `CLUSTER`, `REPLICATED`.                                             | STANDALONE                |
| endpoints.[]         | An list of Redis addresses to use.                                                                               | \[redis://127.0.0.1:6379] |
| keyNamespace         | Optional namespace to prefix all keys with. This is useful if the Redis instance is shared by multiple services. |                           |
| responseTimeout      | Optional response timeout to use for all Redis commands.                                                         |                           |
| username             | Optional username to use when connecting to Redis. Not recommended to be set from JSON.                          |                           |
| password             | Optional password to use when connecting to Redis. Not recommended to be set from JSON.                          |                           |
| locks.[]             | An list of lock configurations.                                                                                  |                           |
| locks.[].name        | The name of the lock for which to apply the configuration. One of `name` or `pattern` must be set.               |                           |
| locks.[].pattern     | The name pattern of the lock(s) for which to apply the configuration.                                            |                           |
| locks.[].leaseTime   | The lease time in milliseconds before the lock is automatically released. Use `-1` to indicate no timeout.       | -1                        |
| maps.[]              | A list of map configurations.                                                                                    |                           |
| maps.[].name         | The name of the lock for which to apply the configuration. One of `name` or `pattern` must be set.               |                           |
| maps.[].pattern      | The name pattern of the map(s) for which to apply the configuration.                                             |                           |
| maps.[].evictionMode | The map eviction mode when size is exceeded. One of `LRU` or `LFU`.                                              | LRU                       |
| maps.[].maxSize      | The max size (number of keys) allowed in the map. Use `0` to indicate no max size.                               | 0                         |

### Configuration API

The `RedisConfig` API can be used to set all configurable properties. It takes default values from the environment and
system properties allow for injection of secrets while remaining configuration can be built at runtime.

```java
import com.retailsvc.vertx.spi.cluster.redis.config.*;

var config = new RedisConfig()
    .setClientType(ClientType.CLUSTER)
    .addAddress("redis://redis1.internal:6379")
    .addAddress("redis://redis2.internal:6379");
```

### Configuration for data types

Redis maps and locks can be configured through the `RedisConfig` API, either in JSON or programmatically. It allows
developers to control properties that aren't available on the `SharedData` API, such as map max size and eviction
policies.

  * Locks and maps are configurable either by name or a regular expression pattern
  * Maps
    * Maximum size
    * Eviction policy when maximum size is reached
    * Time to live is NOT configurable as it is part of the `AsyncMap` API
  * Locks
    * Maximum lease time before auto release


### Instantiate the Cluster Manager

If you use a standalone client without any custom configuration you can rely on service discovery and just create a
clustered vertx instance.

```java
Vertx vertx = Vertx.clusteredVertx(new VertxOptions())
    .toCompletionStage()
    .toCompletableFuture()
    .get();
```

If custom configuration is used, configure and add the cluster manager using the Vertx builder.
In the example below, we load configuration from the environment and a JSON file.

```java
private Vertx vertx;

private RedisConfig loadConfig() {
  try {
    JsonObject json = new JsonObject(Buffer.buffer(Files.readAllBytes(Path.of("redis-config.json"))));
    return new RedisConfig(json);
  } catch (IOException e) {
    throw new UncheckedIOException("Failed to load RedisConfig", e);
  }
}

public void init() {
  ClusterManager clusterManager = new RedisClusterManager(loadConfig);
  Vertx.builder().withClusterManager(clusterManager).buildClustered(ar -> vertx = ar.result());
}
```

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
