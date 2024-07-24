package com.retailsvc.vertx.spi.cluster.redis.config;

/**
 * Eviction mode.
 *
 * @author sasjo
 */
public enum EvictionMode {

  /** Least Recently Used eviction algorithm. */
  LRU,

  /** Least Frequently Used eviction algorithm. */
  LFU,
}
