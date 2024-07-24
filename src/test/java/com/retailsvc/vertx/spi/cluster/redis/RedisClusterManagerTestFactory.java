package com.retailsvc.vertx.spi.cluster.redis;

import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

public class RedisClusterManagerTestFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RedisClusterManagerTestFactory.class);

  public static RedisClusterManager newInstance(GenericContainer<?> redis) {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    LOG.info("Connect to {}", redisUrl);
    return new RedisClusterManager(new RedisConfig().addEndpoint(redisUrl));
  }
}
