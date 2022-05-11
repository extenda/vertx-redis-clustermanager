package io.vertx.spi.cluster.redis.impl;

import io.vertx.spi.cluster.redis.RedisInstance;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisNodes;

/**
 * Redis instance backed by Redisson.
 *
 * @author sasjo
 */
public final class RedissonRedisInstance implements RedisInstance {
  private final RedissonClient redisson;
  private final RedisConfig config;

  /**
   * Create a new instance.
   *
   * @param redisson redisson client
   * @param config redis cluster manager config
   */
  public RedissonRedisInstance(RedissonClient redisson, RedisConfig config) {
    this.redisson = redisson;
    this.config = config;
  }

  @Override
  public boolean ping() {
    if (config.getClientType() == ClientType.STANDALONE) {
      return redisson.getRedisNodes(RedisNodes.SINGLE).pingAll();
    }
    throw new IllegalStateException(
        "Ping is not supported for client type: " + config.getClientType());
  }
}
