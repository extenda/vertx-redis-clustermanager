package io.vertx.spi.cluster.redis.impl;

import io.vertx.spi.cluster.redis.RedisInstance;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
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
  private final RedisKeyFactory keyFactory;

  /**
   * Create a new instance.
   *
   * @param redisson redisson client
   * @param config redis cluster manager config
   * @param keyFactory redis key factory
   */
  public RedissonRedisInstance(
      RedissonClient redisson, RedisConfig config, RedisKeyFactory keyFactory) {
    this.redisson = redisson;
    this.config = config;
    this.keyFactory = keyFactory;
  }

  @Override
  public boolean ping() {
    if (config.getClientType() == ClientType.STANDALONE) {
      return redisson.getRedisNodes(RedisNodes.SINGLE).pingAll();
    }
    throw new IllegalStateException(
        "Ping is not supported for client type: " + config.getClientType());
  }

  @Override
  public <V> BlockingQueue<V> getBlockingQueue(String name) {
    return redisson.getBlockingQueue(keyFactory.build(name));
  }

  @Override
  public <V> BlockingDeque<V> getBlockingDeque(String name) {
    return redisson.getBlockingDeque(keyFactory.build(name));
  }
}
