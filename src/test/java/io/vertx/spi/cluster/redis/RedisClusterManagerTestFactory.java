package io.vertx.spi.cluster.redis;

import io.vertx.spi.cluster.redis.config.RedisConfig;
import org.testcontainers.containers.GenericContainer;

public class RedisClusterManagerTestFactory {
  public static RedisClusterManager newInstance(GenericContainer<?> redis) {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    return new RedisClusterManager(new RedisConfig().addEndpoint(redisUrl));
  }
}
