package io.vertx.spi.cluster.redis.impl;

import static io.vertx.spi.cluster.redis.impl.CloseableLock.lock;

import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.codec.RedisMapCodec;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import jodd.util.StringUtil;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.ReplicatedServersConfig;
import org.redisson.config.SingleServerConfig;
import org.redisson.connection.ConnectionListener;

/**
 * Redisson context with the active Redisson client.
 *
 * @author sasjo
 */
public final class RedissonContext {

  private final Config redisConfig;
  private final RedisKeyFactory keyFactory;
  private final RedisConfig config;
  private final ConcurrentMap<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, SemaphoreWrapper> locksCache = new ConcurrentHashMap<>();

  private RedissonClient client;
  private ExecutorService lockReleaseExec;
  private final AtomicReference<RedissonConnectionListener> connectionListener =
      new AtomicReference<>(null);
  private final Lock lock = new ReentrantLock();

  /**
   * Create a new Redisson context with specified configuration.
   *
   * @param config the redis configuration
   */
  public RedissonContext(RedisConfig config) {
    this(config, RedissonContext.class.getClassLoader());
  }

  /**
   * Create a new Redisson context with specified configuration.
   *
   * @param config the redis configuration
   * @param dataClassLoader class loader used to restore keys and values returned from Redis
   */
  public RedissonContext(RedisConfig config, ClassLoader dataClassLoader) {
    Objects.requireNonNull(dataClassLoader);
    redisConfig = new Config();

    if (config.getClientType() == ClientType.STANDALONE) {
      SingleServerConfig singleServerConfig = redisConfig.useSingleServer();
      singleServerConfig.setAddress(config.getEndpoints().get(0));
      singleServerConfig.setTimeout(10_000);
      singleServerConfig.setRetryAttempts(5);
      singleServerConfig.setRetryInterval(1_000);
      if (StringUtil.isNotEmpty(config.getUsername())) {
        singleServerConfig.setUsername(config.getUsername());
      }
      if (StringUtil.isNotEmpty(config.getPassword())) {
        singleServerConfig.setPassword(config.getPassword());
      }
    } else if (config.getClientType() == ClientType.CLUSTER) {
      ClusterServersConfig clusterServersConfig = redisConfig.useClusterServers();
      clusterServersConfig.setNodeAddresses(config.getEndpoints());
      clusterServersConfig.setTimeout(10_000);
      clusterServersConfig.setRetryAttempts(5);
      clusterServersConfig.setRetryInterval(1_000);

      if (StringUtil.isNotEmpty(config.getUsername())) {
        clusterServersConfig.setUsername(config.getUsername());
      }
      if (StringUtil.isNotEmpty(config.getPassword())) {
        clusterServersConfig.setPassword(config.getPassword());
      }
    } else if (config.getClientType() == ClientType.REPLICATED) {
      ReplicatedServersConfig replicatedServersConfig = redisConfig.useReplicatedServers();
      replicatedServersConfig.setNodeAddresses(config.getEndpoints());
      replicatedServersConfig.setTimeout(10_000);
      replicatedServersConfig.setRetryAttempts(5);
      replicatedServersConfig.setRetryInterval(1_000);

      if (StringUtil.isNotEmpty(config.getUsername())) {
        replicatedServersConfig.setUsername(config.getUsername());
      }
      if (StringUtil.isNotEmpty(config.getPassword())) {
        replicatedServersConfig.setPassword(config.getPassword());
      }
    } else {
     throw new IllegalStateException("ClientType not support: " + config.getClientType());
    }

    if (config.isUseConnectionListener()) {
      redisConfig.setConnectionListener(new DelegateConnectionListener());
    }
    redisConfig.setCodec(new RedisMapCodec(dataClassLoader));
    if (dataClassLoader != getClass().getClassLoader()) {
      redisConfig.setUseThreadClassLoader(false);
    }
    keyFactory = new RedisKeyFactory(config.getKeyNamespace());
    this.config = new RedisConfig(config);
  }

  public RedisConfig config() {
    return config;
  }

  public RedisKeyFactory keyFactory() {
    return keyFactory;
  }

  public RedissonClient client() {
    try (var ignored = lock(lock)) {
      if (client == null) {
        client = Redisson.create(redisConfig);
        lockReleaseExec =
            Executors.newCachedThreadPool(
                Thread.ofPlatform().name("vertx-redis-service-release-lock-thread", 1).factory());
      }
      return client;
    }
  }

  ConcurrentMap<String, SemaphoreWrapper> locksCache() {
    return locksCache;
  }

  ConcurrentMap<String, AsyncMap<?, ?>> asyncMapCache() {
    return asyncMapCache;
  }

  ExecutorService lockReleaseExec() {
    return lockReleaseExec;
  }

  public void shutdown() {
    try (var ignored = lock(lock)) {
      if (client != null) {
        client.shutdown();
        lockReleaseExec.shutdown();
        locksCache.clear();
        asyncMapCache.clear();
      }
      client = null;
      lockReleaseExec = null;
    }
  }

  public void setConnectionListener(RedissonConnectionListener listener) {
    if (config.isUseConnectionListener()) {
      this.connectionListener.set(listener);
    }
  }

  private class DelegateConnectionListener implements ConnectionListener {
    @Override
    public void onConnect(InetSocketAddress addr) {
      Optional.ofNullable(connectionListener.get())
          .ifPresent(RedissonConnectionListener::onConnect);
    }

    @Override
    public void onDisconnect(InetSocketAddress addr) {
      Optional.ofNullable(connectionListener.get())
          .ifPresent(RedissonConnectionListener::onDisconnect);
    }
  }
}
