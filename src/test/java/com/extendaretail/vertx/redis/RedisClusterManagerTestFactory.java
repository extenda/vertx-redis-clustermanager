package com.extendaretail.vertx.redis;

import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;

class RedisClusterManagerTestFactory {
  static RedisClusterManager newInstance(GenericContainer<?> redis) {
    Config config = new Config();
    config
        .useSingleServer()
        .setAddress(String.format("redis://%s:%s", redis.getHost(), redis.getFirstMappedPort()));
    return new RedisClusterManager(config);
  }
}
