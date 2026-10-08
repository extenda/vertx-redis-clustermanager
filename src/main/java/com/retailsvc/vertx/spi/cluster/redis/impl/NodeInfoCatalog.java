package com.retailsvc.vertx.spi.cluster.redis.impl;

import static java.util.stream.Collectors.joining;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;

/**
 * Track Vert.x nodes registration in Redis.
 *
 * @author sasjo
 */
public class NodeInfoCatalog {

  /** Node time-to-live in Redis cache. */
  private static final int TTL_SECONDS = 30;

  private final RMapCache<String, NodeInfo> nodeInfoMap;
  private final String nodeId;
  private final List<Integer> listenerIds = new ArrayList<>();
  private final ScheduledExecutorService heartbeatExecutor;
  private final ExecutorService listenerExecutor;
  private final AtomicReference<NodeInfo> nodeInfo = new AtomicReference<>();

  /**
   * Create the cluster node info catalog.
   *
   * @param vertx the Vertx instance
   * @param redisson the Redisson client
   * @param keyFactory the key factory
   * @param nodeId the unique node ID
   * @param listener a listener for node registration in the cluster
   */
  public NodeInfoCatalog(
      Vertx vertx,
      RedissonClient redisson,
      RedisKeyFactory keyFactory,
      String nodeId,
      NodeInfoCatalogListener listener) {
    Objects.requireNonNull(vertx, "vertx");
    this.nodeId = nodeId;
    nodeInfoMap = redisson.getMapCache(keyFactory.vertx("nodeInfo"));

    listenerExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread = new Thread(r, "vertx-redis-nodeInfo-listener");
              thread.setDaemon(true);
              return thread;
            });

    heartbeatExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "vertx-redis-nodeInfo-heartbeat");
              thread.setDaemon(true);
              return thread;
            });

    // These listeners will detect map modifications from other nodes.
    EntryCreatedListener<String, NodeInfo> entryCreated =
        event -> listenerExecutor.submit(() -> listener.memberAdded(event.getKey()));
    EntryRemovedListener<String, NodeInfo> entryRemoved =
        event -> listenerExecutor.submit(() -> listener.memberRemoved(event.getKey()));
    EntryExpiredListener<String, NodeInfo> entryExpired =
        event -> listenerExecutor.submit(() -> listener.memberRemoved(event.getKey()));

    listenerIds.add(nodeInfoMap.addListener(entryCreated));
    listenerIds.add(nodeInfoMap.addListener(entryRemoved));
    listenerIds.add(nodeInfoMap.addListener(entryExpired));

    // Keep the node alive for TTL_SECONDS on a dedicated thread so cluster listener work cannot
    // delay heartbeats past the map entry TTL.
    heartbeatExecutor.scheduleAtFixedRate(
        this::registerNodeSync, TTL_SECONDS / 2, TTL_SECONDS / 2, TimeUnit.SECONDS);
  }

  /** Register the node in the catalog. This will keep the node alive for TTL_SECONDS. */
  private void registerNodeSync() {
    NodeInfo info = nodeInfo.get();
    if (info != null) {
      nodeInfoMap.fastPut(nodeId, info, TTL_SECONDS, TimeUnit.SECONDS);
    }
  }

  /**
   * Return information about a node in the cluster.
   *
   * @param nodeId the node ID
   * @return the node information or null if not a cluster member
   */
  public NodeInfo get(String nodeId) {
    return nodeInfoMap.get(nodeId);
  }

  /**
   * Store the node information for this running node.
   *
   * @param nodeInfo the node information
   */
  public void setNodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo.set(nodeInfo);
    heartbeatExecutor.execute(this::registerNodeSync);
  }

  /**
   * Remove a node from the cluster.
   *
   * @param nodeId the node ID to remove
   */
  public void remove(String nodeId) {
    nodeInfoMap.fastRemove(nodeId);
  }

  /**
   * Return a list of node identifiers corresponding to the nodes in the cluster.
   *
   * @return a list of node identifiers.
   */
  public List<String> getNodes() {
    return new ArrayList<>(nodeInfoMap.readAllKeySet());
  }

  /** Close the catalog. */
  public void close() {
    listenerIds.forEach(nodeInfoMap::removeListener);
    nodeInfo.set(null);
    shutdownExecutor(heartbeatExecutor);
    shutdownExecutor(listenerExecutor);
  }

  private static void shutdownExecutor(ExecutorService executor) {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    return nodeInfoMap.entrySet().stream()
        .map(
            entry -> {
              StringBuilder sb =
                  new StringBuilder("  - [")
                      .append(entry.getKey())
                      .append("]: ")
                      .append(entry.getValue());
              if (entry.getKey().equals(nodeId)) {
                sb.append(" (self)");
              }
              return sb.toString();
            })
        .collect(joining("\n"));
  }
}
