package io.vertx.spi.cluster.redis.impl;

import static java.util.stream.Collectors.joining;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;

/** Track Vert.x nodes registration in Redis. */
public class NodeInfoCatalog {

  /** Node time-to-live in Redis cache. */
  private static final int TTL_SECONDS = 30;

  private final Lock lock = new ReentrantLock();
  private final RMapCache<String, NodeInfo> nodeInfoMap;
  private final Vertx vertx;
  private final String nodeId;
  private final List<Integer> listenerIds = new ArrayList<>();
  private final long timerId;
  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(r -> new Thread(r, "vertx-redis-nodeInfo-thread"));
  private NodeInfo nodeInfo;

  public NodeInfoCatalog(
      Vertx vertx,
      RedissonClient redisson,
      RedisKeyFactory keyFactory,
      String nodeId,
      NodeInfoCatalogListener listener) {
    this.vertx = vertx;
    this.nodeId = nodeId;
    nodeInfoMap = redisson.getMapCache(keyFactory.vertx("nodeInfo"));

    // These listeners will detect map modifications from other nodes.
    EntryCreatedListener<String, NodeInfo> entryCreated =
        event -> executor.submit(() -> listener.memberAdded(event.getKey()));
    EntryRemovedListener<String, NodeInfo> entryRemoved =
        event -> executor.submit(() -> listener.memberRemoved(event.getKey()));
    EntryExpiredListener<String, NodeInfo> entryExpired =
        event -> executor.submit(() -> listener.memberRemoved(event.getKey()));

    listenerIds.add(nodeInfoMap.addListener(entryCreated));
    listenerIds.add(nodeInfoMap.addListener(entryRemoved));
    listenerIds.add(nodeInfoMap.addListener(entryExpired));

    // This periodic timer will keep the node from expiring as long as the process is running.
    timerId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(TTL_SECONDS / 2), id -> registerNode());
  }

  /** Register the node in the catalog. This will keep the node alive for TTL_SECONDS. */
  private void registerNode() {
    try (var ignored = CloseableLock.lock(lock)) {
      if (nodeInfo != null) {
        nodeInfoMap.fastPut(nodeId, nodeInfo, TTL_SECONDS, TimeUnit.SECONDS);
      }
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
    try (var ignored = CloseableLock.lock(lock)) {
      this.nodeInfo = nodeInfo;
      registerNode();
    }
  }

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

  public void close() {
    try (var ignored = CloseableLock.lock(lock)) {
      listenerIds.forEach(nodeInfoMap::removeListener);
      setNodeInfo(null);
      vertx.cancelTimer(timerId);
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
