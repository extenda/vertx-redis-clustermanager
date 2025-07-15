package com.retailsvc.vertx.spi.cluster.redis;

import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import com.retailsvc.vertx.spi.cluster.redis.impl.CloseableLock;
import com.retailsvc.vertx.spi.cluster.redis.impl.NodeInfoCatalog;
import com.retailsvc.vertx.spi.cluster.redis.impl.NodeInfoCatalogListener;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonContext;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import com.retailsvc.vertx.spi.cluster.redis.impl.SubscriptionCatalog;
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
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.singleton;

/**
 * A Vert.x cluster manager for Redis.
 *
 * @author sasjo
 */
public class RedisClusterManager implements ClusterManager, NodeInfoCatalogListener {

  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);
  private final RedissonContext redissonContext;

  private VertxInternal vertx;
  private NodeSelector nodeSelector;
  private String nodeId;
  private final AtomicReference<NodeInfo> nodeInfo = new AtomicReference<>();
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();
  private final ReentrantLock lock = new ReentrantLock();

  private RedissonRedisInstance dataGrid;

  private NodeInfoCatalog nodeInfoCatalog;
  private SubscriptionCatalog subscriptionCatalog;

  /**
   * Create a Redis cluster manager with default configuration from system properties or environment variables.
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
   * @param config          the redis configuration
   * @param dataClassLoader class loader used to restore keys and values returned from Redis
   */
  public RedisClusterManager(RedisConfig config, ClassLoader dataClassLoader) {
    redissonContext = new RedissonContext(config, dataClassLoader);
  }

  @Override
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    promise.complete(dataGrid.getAsyncMap(name));
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return dataGrid.getMap(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    dataGrid.getLockWithTimeout(name, timeout).onComplete(promise);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    promise.complete(dataGrid.getCounter(name));
  }

  @Override
  public String getNodeId() {
    return nodeId;
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
    vertx
        .<Void>executeBlocking(
            () -> {
              try (var ignored = CloseableLock.lock(lock)) {
                this.nodeInfo.set(nodeInfo);
              }
              nodeInfoCatalog.setNodeInfo(nodeInfo);
              return null;
            },
            false)
        .onComplete(promise);
  }

  @Override
  public NodeInfo getNodeInfo() {
    return nodeInfo.get();
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    vertx
        .executeBlocking(
            () -> {
              NodeInfo value = nodeInfoCatalog.get(nodeId);
              if (value != null) {
                return value;
              } else {
                throw new VertxException("Not a member of the cluster");
              }
            },
            false)
        .onComplete(promise);
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx
        .<Void>executeBlocking(
            () -> {
              if (active.compareAndSet(false, true)) {
                try (var ignored = CloseableLock.lock(lock)) {
                  nodeId = UUID.randomUUID().toString();
                  log.debug("Join cluster as {}", nodeId);
                  dataGrid = new RedissonRedisInstance(vertx, redissonContext);
                  createCatalogs(redissonContext.client());
                }
              } else {
                log.warn("Already activated, nodeId: {}", nodeId);
              }
              return null;
            })
        .onComplete(promise);
  }

  private void createCatalogs(RedissonClient redisson) {
    nodeInfoCatalog =
        new NodeInfoCatalog(vertx, redisson, redissonContext.keyFactory(), nodeId, this);
    if (subscriptionCatalog != null) {
      subscriptionCatalog =
          new SubscriptionCatalog(
              subscriptionCatalog, redisson, redissonContext.keyFactory(), nodeSelector);
    } else {
      subscriptionCatalog =
          new SubscriptionCatalog(redisson, redissonContext.keyFactory(), nodeSelector);
    }
    subscriptionCatalog.removeUnknownSubs(nodeId, nodeInfoCatalog.getNodes());
  }

  private String logId(String nodeId) {
    return nodeId.equals(this.nodeId) ? "%s (self)".formatted(nodeId) : nodeId;
  }

  @Override
  public void memberAdded(String nodeId) {
    if (!isActive())
      return;
    try (var ignored = CloseableLock.lock(lock)) {
      if (log.isDebugEnabled()) {
        log.debug("Add member [{}]", logId(nodeId));
      }
      if (nodeListener != null) {
        nodeListener.nodeAdded(nodeId);
      }
      log.debug("Nodes in catalog:\n{}", nodeInfoCatalog);
    }
  }

  @Override
  public void memberRemoved(String nodeId) {
    if (!isActive())
      return;
    try (var ignored = CloseableLock.lock(lock)) {
      if (log.isDebugEnabled()) {
        log.debug("Remove member [{}]", logId(nodeId));
      }
      subscriptionCatalog.removeAllForNodes(singleton(nodeId));

      log.debug("Nodes in catalog:\n{}", nodeInfoCatalog);

      // Register self again.
      registerSelfAgain();

      if (nodeListener != null) {
        nodeListener.nodeLeft(nodeId);
      }
    }
  }

  /** Re-register self in the cluster. */
  private void registerSelfAgain() {
    try (var ignored = CloseableLock.lock(lock)) {
      nodeInfoCatalog.setNodeInfo(getNodeInfo());
      nodeSelector.registrationsLost();
      subscriptionCatalog.republishOwnSubs();
    }
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx
        .<Void>executeBlocking(
            () -> {
              // We need this to be synchronized to prevent other calls from happening while leaving
              // the cluster, typically memberAdded and memberRemoved.
              if (active.compareAndSet(true, false)) {
                try (var ignored = CloseableLock.lock(lock)) {
                  log.debug("Leave cluster as {}", nodeId);

                  // Stop catalog services.
                  closeCatalogs();

                  // Remove self from cluster.
                  subscriptionCatalog.removeAllForNodes(singleton(nodeId));
                  nodeInfoCatalog.remove(nodeId);

                  // Disconnect from Redis
                  redissonContext.shutdown();
                }
              } else {
                log.warn("Already deactivated, nodeId: {}", nodeId);
              }
              return null;
            })
        .onComplete(promise);
  }

  private void closeCatalogs() {
    subscriptionCatalog.close();
    nodeInfoCatalog.close();
  }

  @Override
  public boolean isActive() {
    return active.get();
  }

  @Override
  public void addRegistration(
      String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    vertx
        .<Void>executeBlocking(
            () -> {
              subscriptionCatalog.put(address, registrationInfo);
              return null;
            },
            false)
        .onComplete(promise);
  }

  @Override
  public void removeRegistration(
      String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    vertx
        .<Void>executeBlocking(
            () -> {
              subscriptionCatalog.remove(address, registrationInfo);
              return null;
            },
            false)
        .onComplete(promise);
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    vertx.executeBlocking(() -> subscriptionCatalog.get(address), false).onComplete(promise);
  }

  /**
   * Returns a Redis instance object. This method returns an empty optional when the cluster manager is inactive.
   *
   * @return the redis instance if active.
   */
  public Optional<RedisInstance> getRedisInstance() {
    if (!isActive()) {
      return Optional.empty();
    }
    return Optional.ofNullable(dataGrid);
  }
}
