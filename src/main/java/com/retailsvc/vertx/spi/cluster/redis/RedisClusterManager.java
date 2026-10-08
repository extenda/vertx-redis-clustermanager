package com.retailsvc.vertx.spi.cluster.redis;

import static java.util.Collections.singleton;

import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import com.retailsvc.vertx.spi.cluster.redis.impl.CloseableLock;
import com.retailsvc.vertx.spi.cluster.redis.impl.NodeInfoCatalog;
import com.retailsvc.vertx.spi.cluster.redis.impl.NodeInfoCatalogListener;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonContext;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import com.retailsvc.vertx.spi.cluster.redis.impl.SubscriptionCatalog;
import io.vertx.core.Completable;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.eventbus.impl.clustered.NodeSelector;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Vert.x cluster manager for Redis.
 *
 * @author sasjo
 */
public class RedisClusterManager implements ClusterManager, NodeInfoCatalogListener {

  private static final Logger log = LoggerFactory.getLogger(RedisClusterManager.class);
  private final RedissonContext redissonContext;

  private VertxInternal vertx;
  private RegistrationListener registrationListener;
  private String nodeId;
  private NodeInfo nodeInfo;
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();
  private final ReentrantLock lock = new ReentrantLock(true);

  private RedissonRedisInstance dataGrid;

  private NodeInfoCatalog nodeInfoCatalog;
  private SubscriptionCatalog subscriptionCatalog;

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
    redissonContext = new RedissonContext(config, dataClassLoader);
  }

  @Override
  public void init(Vertx vertx) {
    this.vertx = (VertxInternal) vertx;
  }

  @Override
  public void registrationListener(RegistrationListener registrationListener) {
    this.registrationListener = registrationListener;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Completable<AsyncMap<K, V>> completable) {
    try {
      completable.succeed(dataGrid.getAsyncMap(name));
    } catch (Throwable t) {
      completable.fail(t);
    }
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return dataGrid.getMap(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Completable<Lock> completable) {
    dataGrid
        .getLockWithTimeout(name, timeout)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed(ar.result());
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  @Override
  public void getCounter(String name, Completable<Counter> completable) {
    try {
      completable.succeed(dataGrid.getCounter(name));
    } catch (Throwable t) {
      completable.fail(t);
    }
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
  public void setNodeInfo(NodeInfo nodeInfo, Completable<Void> completable) {
    vertx
        .<Void>executeBlocking(
            () -> {
              try (var ignored = CloseableLock.lock(lock)) {
                this.nodeInfo = nodeInfo;
              }
              nodeInfoCatalog.setNodeInfo(nodeInfo);
              return null;
            },
            false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed();
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  @Override
  public NodeInfo getNodeInfo() {
    try (var ignored = CloseableLock.lock(lock)) {
      return nodeInfo;
    }
  }

  @Override
  public void getNodeInfo(String nodeId, Completable<NodeInfo> completable) {
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
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed(ar.result());
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  @Override
  public void join(Completable<Void> completable) {
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
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed();
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  private void createCatalogs(RedissonClient redisson) {
    nodeInfoCatalog =
        new NodeInfoCatalog(vertx, redisson, redissonContext.keyFactory(), nodeId, this);
    if (subscriptionCatalog != null) {
      subscriptionCatalog =
          new SubscriptionCatalog(
              subscriptionCatalog,
              redisson,
              redissonContext.keyFactory(),
              (NodeSelector) registrationListener);
    } else {
      subscriptionCatalog =
          new SubscriptionCatalog(
              redisson, redissonContext.keyFactory(), (NodeSelector) registrationListener);
    }
    subscriptionCatalog.removeUnknownSubs(nodeId, nodeInfoCatalog.getNodes());
  }

  private String logId(String nodeId) {
    try (var ignored = CloseableLock.lock(lock)) {
      return nodeId.equals(this.nodeId) ? "%s (self)".formatted(nodeId) : nodeId;
    }
  }

  @Override
  public void memberAdded(String nodeId) {
    try (var ignored = CloseableLock.lock(lock)) {
      if (isActive()) {
        if (log.isDebugEnabled()) {
          log.debug("Add member [{}]", logId(nodeId));
        }
        if (nodeListener != null) {
          nodeListener.nodeAdded(nodeId);
        }
        log.debug("Nodes in catalog:\n{}", nodeInfoCatalog);
      }
    }
  }

  @Override
  public void memberRemoved(String nodeId) {
    try (var ignored = CloseableLock.lock(lock)) {
      if (isActive()) {
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
  }

  /** Re-register self in the cluster. */
  private void registerSelfAgain() {
    try (var ignored = CloseableLock.lock(lock)) {
      nodeInfoCatalog.setNodeInfo(getNodeInfo());
      registrationListener.registrationsLost();

      vertx.executeBlocking(
          () -> {
            subscriptionCatalog.republishOwnSubs();
            return null;
          },
          false);
    }
  }

  @Override
  public void leave(Completable<Void> completable) {
    vertx
        .<Void>executeBlocking(
            () -> {
              // We need this to be synchronized to prevent other calls from happening while leaving
              // the cluster, typically memberAdded and memberRemoved.
              if (active.compareAndSet(true, false)) {
                try (var ignored = CloseableLock.lock(lock)) {
                  log.debug("Leave custer as {}", nodeId);

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
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed();
              } else {
                completable.fail(ar.cause());
              }
            });
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
      String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
    vertx
        .<Void>executeBlocking(
            () -> {
              subscriptionCatalog.put(address, registrationInfo);
              return null;
            },
            false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed();
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  @Override
  public void removeRegistration(
      String address, RegistrationInfo registrationInfo, Completable<Void> completable) {
    vertx
        .<Void>executeBlocking(
            () -> {
              subscriptionCatalog.remove(address, registrationInfo);
              return null;
            },
            false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed();
              } else {
                completable.fail(ar.cause());
              }
            });
  }

  @Override
  public void getRegistrations(String address, Completable<List<RegistrationInfo>> completable) {
    vertx
        .executeBlocking(() -> subscriptionCatalog.get(address), false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                completable.succeed(ar.result());
              } else {
                completable.fail(ar.cause());
              }
            });
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
    return Optional.ofNullable(dataGrid);
  }
}
