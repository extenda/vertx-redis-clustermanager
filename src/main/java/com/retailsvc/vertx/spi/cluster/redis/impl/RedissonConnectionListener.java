package com.retailsvc.vertx.spi.cluster.redis.impl;

/**
 * Listener for changes to the Redis connection status.
 *
 * @author sasjo
 */
public interface RedissonConnectionListener {

  /** Invoked when the Redis connection is established. */
  void onConnect();

  /** Invoked when the Redis connection is lost. */
  void onDisconnect();
}
