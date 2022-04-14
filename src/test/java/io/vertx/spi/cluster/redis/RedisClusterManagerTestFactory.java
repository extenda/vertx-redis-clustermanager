package io.vertx.spi.cluster.redis;

import org.testcontainers.containers.GenericContainer;

public class RedisClusterManagerTestFactory {
  public static RedisClusterManager newInstance(GenericContainer<?> redis) {
    return new RedisClusterManager(
        RedisConfig.withAddress("redis", redis.getHost(), redis.getFirstMappedPort()));
  }
}
