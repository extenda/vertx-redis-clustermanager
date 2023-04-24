package io.vertx.spi.cluster.redis;

import static java.util.Collections.singleton;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
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
import io.vertx.spi.cluster.redis.impl.RedissonContext;
import io.vertx.spi.cluster.redis.impl.RedissonRedisInstance;
import io.vertx.spi.cluster.redis.impl.SubscriptionCatalog;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private UUID nodeId;
  private NodeInfo nodeInfo;
  private NodeListener nodeListener;

  private final AtomicBoolean active = new AtomicBoolean();

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
    synchronized (this) {
      return nodeInfo;
    }
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
            synchronized (this) {
              nodeId = UUID.randomUUID();

              dataGrid = new RedissonRedisInstance(vertx, redissonContext);
              RedissonClient redisson = redissonContext.client();
              nodeInfoCatalog =
                  new NodeInfoCatalog(
                      vertx, redisson, redissonContext.keyFactory(), nodeId.toString(), this);
              subscriptionCatalog =
                  new SubscriptionCatalog(redisson, redissonContext.keyFactory(), nodeSelector);
            }
          } else {
            log.warn("Already activated, nodeId: {}", nodeId);
          }
          prom.complete();
        },
        promise);
  }

  @Override
  public synchronized void memberAdded(String nodeId) {
    if (isActive()) {
      log.debug("Add member [{}]", nodeId);
      if (nodeListener != null) {
        nodeListener.nodeAdded(nodeId);
      }
      log.debug("Nodes in catalog:\n{}", nodeInfoCatalog);
    }
  }

  @Override
  public synchronized void memberRemoved(String nodeId) {
    if (isActive()) {
      log.debug("Remove member [{}]", nodeId);
      subscriptionCatalog.removeAllForNodes(singleton(nodeId));
      nodeInfoCatalog.remove(nodeId);
      log.debug("Nodes in catalog:\n{}", nodeInfoCatalog);

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
          // We need this to be synchronized to prevent other calls from happening while leaving the
          // cluster, typically memberAdded and memberRemoved.
          if (active.compareAndSet(true, false)) {
            synchronized (RedisClusterManager.this) {
              try {
                // Stop catalog services.
                subscriptionCatalog.close();
                nodeInfoCatalog.close();

                // Remove self from cluster.
                subscriptionCatalog.removeAllForNodes(singleton(nodeId.toString()));
                nodeInfoCatalog.remove(nodeId.toString());

                // Disconnect from Redis
                redissonContext.shutdown();
              } catch (Exception e) {
                prom.fail(e);
              }
            }
          } else {
            log.warn("Already deactivated, nodeId: {}", nodeId);
          }
          prom.tryComplete();
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
    return Optional.ofNullable(dataGrid);
  }
}
