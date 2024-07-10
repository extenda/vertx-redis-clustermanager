package io.vertx.spi.cluster.redis.config;

/**
 * Redis client type.
 *
 * @author sasjo
 */
public enum ClientType {

  /** The client should work in single server mode (the default). */
  STANDALONE,
  CLUSTER,
  REPLICATED,
  MASTER_SLAVE,
  SENTINEL_MASTER_SLAVE,
}
