package com.retailsvc.vertx.spi.cluster.redis;

import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonContext;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

/**
 * Redis backed data grid. The data grid can be used without the {@link RedisClusterManager}.
 *
 * @author sasjo
 */
public interface RedisDataGrid {

  /**
   * Create a data grid backed by Redis.
   *
   * @param vertx the vertx context
   * @param config the redis configuration
   * @return a data grid backed by Redis.
   */
  static RedisDataGrid create(Vertx vertx, RedisConfig config) {
    return create(vertx, config, RedisDataGrid.class.getClassLoader());
  }

  /**
   * Create a data grid backed by Redis.
   *
   * @param vertx the vertx context
   * @param config the redis configuration
   * @param dataClassLoader class loader used to restore keys and values returned from Redis
   * @return a data grid backed by Redis.
   */
  static RedisDataGrid create(Vertx vertx, RedisConfig config, ClassLoader dataClassLoader) {
    return new RedissonRedisInstance(vertx, new RedissonContext(config, dataClassLoader));
  }

  /**
   * Get an {@link AsyncMap} with the specified name. The map is accessible to all nodes connected
   * to the same Redis instance and data put into the map from any node is visible to any other
   * node.
   *
   * <p><strong>Warning:</strong> The map can only store data structures that can be serialized to
   * Redis. Also keep in mind that latency of a distributed map is slower than that of a local map.
   *
   * @param name the name of the map
   * @return the distributed async map.
   * @param <K> the key type
   * @param <V> the value type
   */
  <K, V> AsyncMap<K, V> getAsyncMap(String name);

  /**
   * Like {@link #getAsyncMap(String)} but exposed as a regular {@link Map}. Prefer {@link
   * #getAsyncMap(String)} and use asynchronous API calls to not block on API calls.
   *
   * @param name the name of the map
   * @return the distributed map.
   * @param <K> the key type
   * @param <V> the value type
   */
  <K, V> Map<K, V> getMap(String name);

  /**
   * Get a counter. The counter value is seen by all nodes connected to the same Redis and changes
   * to the counter are visible to in all nodes.
   *
   * @param name the name of the counter
   * @return the distributed counter.
   */
  Counter getCounter(String name);

  /**
   * Get an asynchronous lock with the specified name.
   *
   * @param name the name of the lock
   * @return a future of the resulting lock.
   */
  Future<Lock> getLock(String name);

  /**
   * Like {@link #getLock(String)} but specifying a timeout. If the lock is not obtained within the
   * timeout a failure will be sent to the handler.
   *
   * @param name the name of the lock
   * @param timeout the timeout in ms
   * @return a future of the resulting lock
   */
  Future<Lock> getLockWithTimeout(String name, long timeout);

  /**
   * Returns an unbounded blocking queue backed by Redis.
   *
   * @param <V> type of value
   * @param name name of the queue
   * @return A blocking queue instance.
   */
  <V> BlockingQueue<V> getBlockingQueue(String name);

  /**
   * Returns an unbounded blocking deque backed by Redis.
   *
   * @param <V> type of value
   * @param name name of the deque
   * @return a blocking deque instance.
   */
  <V> BlockingDeque<V> getBlockingDeque(String name);

  /**
   * Returns a topic backed by Redis.
   *
   * @param <V> type of messages in topic
   * @param type the message type of the topic
   * @param name the name of the topic
   * @return a topic instance,
   */
  <V> Topic<V> getTopic(Class<V> type, String name);

  /**
   * Shutdown the data grid connection with Redis. After this, future operations on data grid data
   * types will fail. Use this method to gracefully terminate the Redis connection as part of the
   * application shutdown sequence.
   */
  void shutdown();
}
