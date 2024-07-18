package io.vertx.spi.cluster.redis.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.spi.cluster.redis.Container;
import io.vertx.spi.cluster.redis.RedisInstance;
import io.vertx.spi.cluster.redis.Topic;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.LockConfig;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisCounter;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisLock;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import org.redisson.api.EvictionMode;
import org.redisson.api.RMapCache;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisNodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis instance backed by Redisson.
 *
 * @author sasjo
 * @see io.vertx.spi.cluster.redis.RedisClusterManager
 * @see io.vertx.spi.cluster.redis.RedisDataGrid
 */
public final class RedissonRedisInstance implements RedisInstance {

  private static final Logger log = LoggerFactory.getLogger(RedissonRedisInstance.class);
  private static final long DEFAULT_LOCK_TIMEOUT = 10 * 1000L;

  private final Vertx vertx;
  private final RedisConfig config;
  private final RedisKeyFactory keyFactory;
  private final RedissonContext redissonContext;
  private final RedissonClient redisson;

  /**
   * Create a new instance.
   *
   * @param vertx the Vertx context
   * @param redissonContext the Redisson context
   */
  public RedissonRedisInstance(Vertx vertx, RedissonContext redissonContext) {
    this.vertx = vertx;
    this.redissonContext = redissonContext;
    this.redisson = redissonContext.client();
    this.config = redissonContext.config();
    this.keyFactory = redissonContext.keyFactory();
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
  public <K, V> Map<K, V> getMap(String name) {
    return getMapCache(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncMap<K, V> getAsyncMap(String name) {
    return (AsyncMap<K, V>)
        redissonContext
            .asyncMapCache()
            .computeIfAbsent(name, key -> new RedisAsyncMap<>(vertx, getMapCache(key)));
  }

  private <K, V> RMapCache<K, V> getMapCache(String name) {
    RMapCache<K, V> map = redisson.getMapCache(keyFactory.map(name));
    log.debug("Create map '{}'", name);
    config
        .getMapConfig(name)
        .ifPresent(
            mapConfig -> {
              log.debug("Configure map '{}' with {}", name, mapConfig);
              map.setMaxSize(
                  mapConfig.getMaxSize(), EvictionMode.valueOf(mapConfig.getEvictionMode().name()));
            });
    return map;
  }

  @Override
  public Counter getCounter(String name) {
    return new RedisCounter(vertx, redisson.getAtomicLong(keyFactory.counter(name)));
  }

  @Override
  public Future<Lock> getLock(String name) {
    return getLockWithTimeout(name, DEFAULT_LOCK_TIMEOUT);
  }

  @Override
  public Future<Lock> getLockWithTimeout(String name, long timeout) {
    return vertx.executeBlocking(
        () -> {
          SemaphoreWrapper semaphore =
              redissonContext.locksCache().computeIfAbsent(name, this::createSemaphore);
          RedisLock lock;
          long remaining = timeout;
          do {
            long start = System.nanoTime();
            try {
              lock = semaphore.tryAcquire(remaining, redissonContext.lockReleaseExec());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new VertxException("Interrupted while waiting for lock.", e);
            }
            remaining = remaining - MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
          } while (lock == null && remaining > 0);
          if (lock != null) {
            return lock;
          } else {
            throw new VertxException("Timed out waiting to get lock " + name);
          }
        },
        false);
  }

  private SemaphoreWrapper createSemaphore(String name) {
    int leaseTime =
        redissonContext.config().getLockConfig(name).map(LockConfig::getLeaseTime).orElse(-1);
    if (leaseTime == -1) {
      log.debug("Create semaphore '{}'", name);
      RSemaphore semaphore = redisson.getSemaphore(keyFactory.lock(name));
      semaphore.trySetPermits(1);
      return new SemaphoreWrapper(semaphore);
    }

    log.debug("Create semaphore '{}' with leaseTime={}", name, leaseTime);
    RPermitExpirableSemaphore semaphore =
        redisson.getPermitExpirableSemaphore(keyFactory.lock(name));
    semaphore.trySetPermits(1);
    return new SemaphoreWrapper(semaphore, leaseTime);
  }

  @Override
  public <V> BlockingQueue<V> getBlockingQueue(String name) {
    return redisson.getBlockingQueue(keyFactory.build(name));
  }

  @Override
  public <V> BlockingDeque<V> getBlockingDeque(String name) {
    return redisson.getBlockingDeque(keyFactory.build(name));
  }

  @Override
  public <V extends Serializable> Container<V> getContainer(String name) {
    String bucketName = keyFactory.container(name);
    return new RedisContainer<>(vertx, redisson.getBucket(bucketName));
  }

  @Override
  public <V> Topic<V> getTopic(Class<V> type, String name) {
    return new RedisTopic<>(vertx, type, redisson.getTopic(keyFactory.build(name)));
  }

  @Override
  public void shutdown() {
    redissonContext.shutdown();
  }
}
