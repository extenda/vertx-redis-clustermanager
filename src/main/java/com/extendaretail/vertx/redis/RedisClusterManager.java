package com.extendaretail.vertx.redis;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.extendaretail.vertx.redis.impl.NodeInfoCatalog;
import com.extendaretail.vertx.redis.impl.NodeInfoCatalogListener;
import com.extendaretail.vertx.redis.impl.RedisAsyncMap;
import com.extendaretail.vertx.redis.impl.RedisConfigProps;
import com.extendaretail.vertx.redis.impl.RedisCounter;
import com.extendaretail.vertx.redis.impl.RedisKeyFactory;
import com.extendaretail.vertx.redis.impl.RedisLock;
import com.extendaretail.vertx.redis.impl.RedisMapCodec;
import com.extendaretail.vertx.redis.impl.SubscriptionCatalog;
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
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Vert.x cluster manager for Redis. */
public class RedisClusterManager implements ClusterManager, NodeInfoCatalogListener {

  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);

  private VertxInternal vertx;
  private NodeSelector nodeSelector;
  private UUID nodeId;
  private NodeInfo nodeInfo;
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();

  private final boolean customRedissonClient;
  private Config redisConfig;
  private RedissonClient redisson;

  private NodeInfoCatalog nodeInfoCatalog;
  private SubscriptionCatalog subscriptionCatalog;
  private ExecutorService lockReleaseExec;

  /** Create a Redis cluster manager configured from system properties or environment variables. */
  public RedisClusterManager() {
    this(RedisConfigProps.createRedissonConfig());
  }

  public RedisClusterManager(Config config) {
    this.redisConfig = config;
    this.customRedissonClient = false;
  }

  // Consider removing this to not leak any Redisson impl details.
  public RedisClusterManager(RedissonClient redisson) {
    this.redisson = redisson;
    this.customRedissonClient = true;
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  private <K, V> RMapCache<K, V> getMapCache(String name) {
    return redisson.getMapCache(RedisKeyFactory.INSTANCE.map(name), RedisMapCodec.INSTANCE);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    promise.complete(new RedisAsyncMap<>(vertx, getMapCache(name)));
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return getMapCache(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    vertx.executeBlocking(
        prom -> {
          RSemaphore semaphore = redisson.getSemaphore(RedisKeyFactory.INSTANCE.lock(name));
          semaphore.trySetPermits(1);
          boolean locked;
          long remaining = timeout;
          do {
            long start = System.nanoTime();
            try {
              locked = semaphore.tryAcquire(remaining, MILLISECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new VertxException("Interrupted while waiting for lock.", e);
            }
            remaining = remaining - MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
          } while (!locked && remaining > 0);
          if (locked) {
            prom.complete(new RedisLock(semaphore, lockReleaseExec));
          } else {
            throw new VertxException("Timed out waiting to get lock " + name);
          }
        },
        false,
        promise);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    promise.complete(
        new RedisCounter(vertx, redisson.getAtomicLong(RedisKeyFactory.INSTANCE.counter(name))));
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

            if (!customRedissonClient) {
              redisson = Redisson.create(redisConfig);
            }

            nodeInfoCatalog = new NodeInfoCatalog(vertx, redisson, nodeId.toString(), this);
            subscriptionCatalog = new SubscriptionCatalog(vertx, redisson, nodeSelector);
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

  public RedisInstance getRedisInstance() {
    return new RedissonRedisInstance(redisson);
  }

  private static class RedissonRedisInstance implements RedisInstance {
    private final RedissonClient redisson;

    private RedissonRedisInstance(RedissonClient redisson) {
      this.redisson = redisson;
    }

    @Override
    public DistributedLock getLock(String name) {
      RLock lock = redisson.getLock(RedisKeyFactory.INSTANCE.lock(name));
      return (DistributedLock)
          Proxy.newProxyInstance(
              lock.getClass().getClassLoader(),
              new Class<?>[] {DistributedLock.class},
              (proxy, method, args) -> method.invoke(lock, args));
    }
  }
}
