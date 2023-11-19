package io.vertx.spi.cluster.redis;

import static io.vertx.spi.cluster.redis.impl.CloseableLock.lock;
import static java.util.Collections.singleton;

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
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.NodeInfoCatalog;
import io.vertx.spi.cluster.redis.impl.NodeInfoCatalogListener;
import io.vertx.spi.cluster.redis.impl.RedissonConnectionListener;
import io.vertx.spi.cluster.redis.impl.RedissonContext;
import io.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import io.vertx.spi.cluster.redis.impl.SubscriptionCatalog;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
  private NodeSelector nodeSelector;
  private String nodeId;
  private NodeInfo nodeInfo;
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();
  private final ReentrantLock lock = new ReentrantLock();

  private RedissonRedisInstance dataGrid;

  private NodeInfoCatalog nodeInfoCatalog;
  private SubscriptionCatalog subscriptionCatalog;

  /** Visible for test. */
  RedissonConnectionListener reconnectListener;

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

  /**
   * Create a Redis cluster manager with specified context. Intended for use with tests.
   *
   * @param redissonContext the redisson context
   */
  RedisClusterManager(RedissonContext redissonContext) {
    this.redissonContext = redissonContext;
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
    try (var ignored = lock(lock)) {
      this.nodeInfo = nodeInfo;
    }
    vertx
        .<Void>executeBlocking(
            () -> {
              nodeInfoCatalog.setNodeInfo(nodeInfo);
              return null;
            },
            false)
        .onComplete(promise);
  }

  @Override
  public NodeInfo getNodeInfo() {
    try (var ignored = lock(lock)) {
      return nodeInfo;
    }
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

  RedissonConnectionListener createConnectionListener() {
    return new ReconnectListener();
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx
        .<Void>executeBlocking(
            () -> {
              if (active.compareAndSet(false, true)) {
                try (var ignored = lock(lock)) {
                  nodeId = UUID.randomUUID().toString();
                  log.info("Join cluster as {}", nodeId);
                  reconnectListener = createConnectionListener();
                  redissonContext.setConnectionListener(reconnectListener);
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
    try (var ignored = lock(lock)) {
      return nodeId.equals(this.nodeId) ? "%s (self)".formatted(nodeId) : nodeId;
    }
  }

  @Override
  public void memberAdded(String nodeId) {
    try (var ignored = lock(lock)) {
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
    try (var ignored = lock(lock)) {
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
    try (var ignored = lock(lock)) {
      nodeInfoCatalog.setNodeInfo(getNodeInfo());
      nodeSelector.registrationsLost();

      vertx.executeBlocking(
          () -> {
            subscriptionCatalog.republishOwnSubs();
            return null;
          },
          false);
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
                try (var ignored = lock(lock)) {
                  log.info("Leave custer as {}", nodeId);

                  // Stop catalog services.
                  closeCatalogs();

                  // Detach connection listener
                  redissonContext.setConnectionListener(null);
                  reconnectListener = null;

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

  /**
   * A Redisson connection listener. This listener will re-register Vertx event bus addresses with
   * the effect of re-joining the cluster again. If we loose the Redis connection, we need to
   * trigger refresh to ensure our in-memory state is consistent with the Redis data.
   *
   * <p>Visible for tests
   */
  class ReconnectListener implements RedissonConnectionListener {

    /** Milliseconds to delay the reconnect after a connection is established. */
    private static final long RECONNECT_DELAY = 100;

    /** Time period during which a disconnect/connect cycle is ignored. */
    private static final long CONNECTION_GRACE_PERIOD = 100;

    private static final long NOT_DISCONNECTED_TIME = -1L;

    private final AtomicReference<Long> disconnectTime =
        new AtomicReference<>(NOT_DISCONNECTED_TIME);

    /** Available for tests. */
    final AtomicReference<String> reconnectStatus = new AtomicReference<>(null);

    final AtomicBoolean disconnected = new AtomicBoolean(false);

    final AtomicBoolean reconnectInProgress = new AtomicBoolean(false);

    /** Reconnect to the cluster again. This runs on the Vertx Blocking Executor Thread. */
    void reconnect() {
      try {
        log.info("Reconnecting with Redis...");
        reconnectStatus.set("reconnecting");
        closeCatalogs();
        createCatalogs(redissonContext.client());
        registerSelfAgain();
        reconnectStatus.set("success");
        log.info("Redis connection re-established");
        reconnectInProgress.set(false);
      } catch (Exception e) {
        log.error("Caught exception on reconnect. A retry is scheduled.", e);
        reconnectStatus.set("failure");
        reconnectWithDelay();
      }
    }

    private void reconnectWithDelay() {
      vertx.setTimer(
          RECONNECT_DELAY,
          id ->
              vertx.<Void>executeBlocking(
                  () -> {
                    reconnect();
                    return null;
                  }));
    }

    @Override
    public void onConnect() {
      log.debug("Redis connection up");
      if (disconnected.compareAndSet(true, false)) {
        long disconnectedAt = disconnectTime.getAndSet(NOT_DISCONNECTED_TIME);
        if (disconnectedAt > NOT_DISCONNECTED_TIME
            && System.currentTimeMillis() - disconnectedAt >= CONNECTION_GRACE_PERIOD
            && reconnectInProgress.compareAndSet(false, true)) {
          // Re-establish the cluster connection if we've been disconnected long enough.
          log.trace("Start new reconnect timer from onConnect");
          reconnectWithDelay();
        }
      } else {
        disconnectTime.set(NOT_DISCONNECTED_TIME);
      }
    }

    @Override
    public void onDisconnect() {
      if (disconnected.compareAndSet(false, true)) {
        log.debug("Redis connection lost!");
        disconnectTime.set(System.currentTimeMillis());
      }
    }
  }
}
