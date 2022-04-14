# vertx-redis-clustermanager

A Vert.x ClusterManager for Redis. The cluster manager allows teams to use
Redis as a drop-in replacement for Hazelcast to run Vert.x in clustered
high availability mode.

The cluster manager is designed for use with [Google Cloud MemoryStore for Redis](https://cloud.google.com/memorystore/docs/redis).

## Usage

```xml
<dependency>
  <groupId>com.extendaretail</groupId>
  <artifactId>vertx-redis-clustermanager</artifactId>
  <version>VERSION</version>
</dependency>
```

## Development

The project is built with OpenJDK 8 and Maven.

```bash
mvn verify
```
