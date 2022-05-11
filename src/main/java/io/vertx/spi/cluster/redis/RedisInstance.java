package io.vertx.spi.cluster.redis;

/**
 * Low-level access to distributed data types backed by Redis. Prefer Vertx shared data types over
 * using this API.
 *
 * @author sasjo
 */
public interface RedisInstance {

  /**
   * Ping the Redis instance(s) to determine availability.
   *
   * @return <code>true</code> if ping is successful, otherwise <code>false</code>.
   */
  boolean ping();
}
