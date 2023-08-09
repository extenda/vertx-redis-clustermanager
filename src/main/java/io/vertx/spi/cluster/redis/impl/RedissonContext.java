package io.vertx.spi.cluster.redis.impl;

import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.codec.RedisMapCodec;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
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
  private final CopyOnWriteArrayList<RedissonConnectionListener> listeners =
      new CopyOnWriteArrayList<>();

  private RedissonClient client;
  private ExecutorService lockReleaseExec;

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
      redisConfig.useSingleServer().setAddress(config.getEndpoints().get(0));
    } else {
      throw new IllegalStateException("RedissonContext only supports STANDALONE client");
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
    synchronized (this) {
      if (client == null) {
        client = Redisson.create(redisConfig);
        lockReleaseExec =
            Executors.newCachedThreadPool(
                r -> new Thread(r, "vertx-redis-service-release-lock-thread"));
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
    synchronized (this) {
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

  public void addConnectionListener(RedissonConnectionListener listener) {
    if (config.isUseConnectionListener()) {
      listeners.addIfAbsent(listener);
    }
  }

  public void removeConnectionListener(RedissonConnectionListener listener) {
    if (config.isUseConnectionListener()) {
      listeners.remove(listener);
    }
  }

  private class DelegateConnectionListener implements ConnectionListener {
    @Override
    public void onConnect(InetSocketAddress addr) {
      listeners.forEach(RedissonConnectionListener::onConnect);
    }

    @Override
    public void onDisconnect(InetSocketAddress addr) {
      listeners.forEach(RedissonConnectionListener::onDisconnect);
    }
  }
}
