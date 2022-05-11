package io.vertx.spi.cluster.redis;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.spi.cluster.redis.config.ClientType;
import io.vertx.spi.cluster.redis.config.LockConfig;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.NodeInfoCatalog;
import io.vertx.spi.cluster.redis.impl.NodeInfoCatalogListener;
import io.vertx.spi.cluster.redis.impl.RedisKeyFactory;
import io.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import io.vertx.spi.cluster.redis.impl.SubscriptionCatalog;
import io.vertx.spi.cluster.redis.impl.codec.RedisMapCodec;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisAsyncMap;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisCounter;
import io.vertx.spi.cluster.redis.impl.shareddata.RedisLock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.Redisson;
import org.redisson.api.EvictionMode;
import org.redisson.api.RMapCache;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Vert.x cluster manager for Redis.
 *
 * @author sasjo
 */
public class RedisClusterManager implements ClusterManager, NodeInfoCatalogListener {

  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

  private VertxInternal vertx;
  private NodeSelector nodeSelector;
  private UUID nodeId;
  private NodeInfo nodeInfo;
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();

  private final RedisConfig config;
  private final Config redisConfig;

  private final RedisKeyFactory keyFactory;
  private RedissonClient redisson;

  private NodeInfoCatalog nodeInfoCatalog;
  private SubscriptionCatalog subscriptionCatalog;
  private ExecutorService lockReleaseExec;

  private final ConcurrentMap<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, SemaphoreWrapper> locksCache = new ConcurrentHashMap<>();

  /**
   * Create a Redis cluster manager with default configuration from system properties or environment
   * variables.
   */
  public RedisClusterManager() {
    this(new RedisConfig());
  }

  /**
   * Create a Redis cluster manager with specified configuration.
   *
   * @param config the redis configuration
   */
  public RedisClusterManager(RedisConfig config) {
    this(config, RedisClusterManager.class.getClassLoader());
  }

  /**
   * Create a Redis cluster manager with specified configuration.
   *
   * @param config the redis configuration
   * @param dataClassLoader class loader used to restore keys and values returned from Redis
   */
  public RedisClusterManager(RedisConfig config, ClassLoader dataClassLoader) {
    Objects.requireNonNull(dataClassLoader);
    redisConfig = new Config();

    if (config.getClientType() == ClientType.STANDALONE) {
      redisConfig.useSingleServer().setAddress(config.getEndpoints().get(0));
    } else {
      throw new IllegalStateException("RedisClusterManager only supports STANDALONE client");
    }

    redisConfig.setCodec(new RedisMapCodec(dataClassLoader));
    if (dataClassLoader != getClass().getClassLoader()) {
      redisConfig.setUseThreadClassLoader(false);
    }
    keyFactory = new RedisKeyFactory(config.getKeyNamespace());
    this.config = new RedisConfig(config);
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
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
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    @SuppressWarnings("unchecked")
    AsyncMap<K, V> map =
        (AsyncMap<K, V>)
            asyncMapCache.computeIfAbsent(
                name, key -> new RedisAsyncMap<>(vertx, getMapCache(key)));
    promise.complete(map);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return getMapCache(name);
  }

  private SemaphoreWrapper createSemaphore(String name) {
    int leaseTime = config.getLockConfig(name).map(LockConfig::getLeaseTime).orElse(-1);
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
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    vertx.executeBlocking(
        prom -> {
          SemaphoreWrapper semaphore = locksCache.computeIfAbsent(name, this::createSemaphore);
          RedisLock lock;
          long remaining = timeout;
          do {
            long start = System.nanoTime();
            try {
              lock = semaphore.tryAcquire(remaining, lockReleaseExec);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new VertxException("Interrupted while waiting for lock.", e);
            }
            remaining = remaining - MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
          } while (lock == null && remaining > 0);
          if (lock != null) {
            prom.complete(lock);
          } else {
            throw new VertxException("Timed out waiting to get lock " + name);
          }
        },
        false,
        promise);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    promise.complete(new RedisCounter(vertx, redisson.getAtomicLong(keyFactory.counter(name))));
  }

  @Override
  public String getNodeId() {
    return nodeId.toString();
  }

  @Override
  public List<String> getNodes() {
    return nodeInfoCatalog.getNodes();
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    vertx.executeBlocking(
        prom -> {
          nodeInfoCatalog.setNodeInfo(nodeInfo);
          prom.complete();
        },
        false,
        promise);
  }

  @Override
  public NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    vertx.executeBlocking(
        prom -> {
          NodeInfo value = nodeInfoCatalog.get(nodeId);
          if (value != null) {
            prom.complete(value);
          } else {
            prom.fail("Not a member of the cluster");
          }
        },
        false,
        promise);
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.executeBlocking(
        prom -> {
          if (active.compareAndSet(false, true)) {
            nodeId = UUID.randomUUID();
            lockReleaseExec =
                Executors.newCachedThreadPool(
                    r -> new Thread(r, "vertx-redis-service-release-lock-thread"));

            redisson = Redisson.create(redisConfig);
            nodeInfoCatalog =
                new NodeInfoCatalog(vertx, redisson, keyFactory, nodeId.toString(), this);
            subscriptionCatalog =
                new SubscriptionCatalog(vertx, redisson, keyFactory, nodeSelector);
          } else {
            log.warn("Already activated, nodeId: {}", nodeId);
          }
          prom.complete();
        },
        promise);
  }

  @Override
  public void memberAdded(String nodeId) {
    if (isActive()) {
      log.debug("Add member [{}]", nodeId);
      if (nodeListener != null) {
        nodeListener.nodeAdded(nodeId);
      }
    }
  }

  @Override
  public void memberRemoved(String nodeId) {
    if (isActive()) {
      log.debug("Remove member [{}]", nodeId);
      subscriptionCatalog.removeAllForNodes(singleton(nodeId));
      nodeInfoCatalog.remove(nodeId);

      // Register self again.
      nodeInfoCatalog.setNodeInfo(getNodeInfo());
      nodeSelector.registrationsLost();

      vertx.executeBlocking(
          prom -> {
            subscriptionCatalog.republishOwnSubs();
            prom.complete();
          },
          false);

      if (nodeListener != null) {
        nodeListener.nodeLeft(nodeId);
      }
    }
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.executeBlocking(
        prom -> {
          if (active.compareAndSet(true, false)) {
            try {
              lockReleaseExec.shutdown();

              // Stop catalog services.
              subscriptionCatalog.close();
              nodeInfoCatalog.close();

              // Remove self from cluster.
              subscriptionCatalog.removeAllForNodes(singleton(nodeId.toString()));
              nodeInfoCatalog.remove(nodeId.toString());

              redisson.shutdown();
              redisson = null;
            } catch (Exception e) {
              prom.fail(e);
            }
          } else {
            log.warn("Already deactivated, nodeId: {}", nodeId);
          }
          prom.complete();
        },
        promise);
  }

  @Override
  public boolean isActive() {
    return active.get();
  }

  @Override
  public void addRegistration(
      String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    vertx.executeBlocking(
        prom -> {
          subscriptionCatalog.put(address, registrationInfo);
          prom.complete();
        },
        false,
        promise);
  }

  @Override
  public void removeRegistration(
      String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    vertx.executeBlocking(
        prom -> {
          subscriptionCatalog.remove(address, registrationInfo);
          prom.complete();
        },
        false,
        promise);
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    vertx.executeBlocking(prom -> prom.complete(subscriptionCatalog.get(address)), false, promise);
  }

  /**
   * Returns a Redis instance object. This method returns an empty optional when the cluster manager
   * is inactive.
   *
   * @return the redis instance if active.
   */
  public Optional<RedisInstance> getRedisInstance() {
    if (!isActive()) {
      return Optional.empty();
    }
    return Optional.of(new RedissonRedisInstance(redisson, config));
  }

  /** A redis semaphore wrapper that supports lock lease time when configured. */
  private static class SemaphoreWrapper {
    private RPermitExpirableSemaphore permitSemaphore;
    private RSemaphore semaphore;
    private int leaseTime;

    private SemaphoreWrapper(RSemaphore semaphore) {
      this.semaphore = semaphore;
    }

    private SemaphoreWrapper(RPermitExpirableSemaphore semaphore, int leaseTime) {
      this.permitSemaphore = semaphore;
      this.leaseTime = leaseTime;
    }

    /**
     * Try to acquire a redis lock.
     *
     * @param waitTime max wait time in milliseconds
     * @param lockReleaseExec lock release thread
     * @return teh acquired lock or <code>null</code> if unsuccessful
     * @throws InterruptedException if interrupted while waiting for lock
     */
    public RedisLock tryAcquire(long waitTime, ExecutorService lockReleaseExec)
        throws InterruptedException {
      RedisLock lock = null;
      if (semaphore != null) {
        if (semaphore.tryAcquire(waitTime, MILLISECONDS)) {
          lock = new RedisLock(semaphore, lockReleaseExec);
        }
      } else {
        String permitId = permitSemaphore.tryAcquire(waitTime, leaseTime, MILLISECONDS);
        if (permitId != null) {
          lock = new RedisLock(permitSemaphore, permitId, lockReleaseExec);
        }
      }
      return lock;
    }
  }
}
