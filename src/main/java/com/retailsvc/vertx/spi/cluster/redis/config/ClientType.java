package com.retailsvc.vertx.spi.cluster.redis.config;

/**
 * Redis client type.
 *
 * @author sasjo
 */
public enum ClientType {

  /**
   * The client should work in single server mode (the default).
   *
   * <p>Supports:
   *
   * <ul>
   *   <li>Redis Community Edition
   *   <li>Google Cloud MemoryStore for Redis
   *   <li>Azure Redis Cache
   * </ul>
   */
  STANDALONE,

  /**
   * The client should work in cluster mode.
   *
   * <p>Supports:
   *
   * <ul>
   *   <li>AWS ElastiCache Cluster
   *   <li>Amazon MemoryDB
   *   <li>Azure Redis Cache
   * </ul>
   */
  CLUSTER,

  /**
   * The client should work in replicated mode. With Replicated mode the role of each node is polled
   * to determine if a failover has occurred resulting in a new master.
   *
   * <p>Supports:
   *
   * <ul>
   *   <li>Google Cloud MemoryStore for Redis High Availability.
   *   <li>AWS ElastiCache (non clustered)
   *   <li>Azure Redis Cache (non clustered)
   * </ul>
   */
  REPLICATED,
}
