package com.extendaretail.vertx.redis.impl;

import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;

/** Track Vert.x nodes registration in Redis. */
public class NodeInfoCatalog {

  /** Node time-to-live in Redis cache. */
  private static final int TTL_SECONDS = 30;

  private final RMapCache<String, NodeInfo> nodeInfoMap;
  private final Vertx vertx;
  private final String nodeId;
  private final List<Integer> listenerIds = new ArrayList<>();
  private final long timerId;

  public NodeInfoCatalog(
      Vertx vertx, RedissonClient redisson, String nodeId, NodeInfoCatalogListener listener) {
    this.vertx = vertx;
    this.nodeId = nodeId;
    nodeInfoMap =
        redisson.getMapCache(RedisKeyFactory.INSTANCE.vertx("nodeInfo"), RedisMapCodec.INSTANCE);

    // These listeners will detect map modifications from other nodes.
    EntryCreatedListener<String, NodeInfo> entryCreated =
        event -> listener.memberAdded(event.getKey());
    EntryRemovedListener<String, NodeInfo> entryRemoved =
        event -> listener.memberRemoved(event.getKey());
    EntryExpiredListener<String, NodeInfo> entryExpired =
        event -> listener.memberRemoved(event.getKey());

    listenerIds.add(nodeInfoMap.addListener(entryCreated));
    listenerIds.add(nodeInfoMap.addListener(entryRemoved));
    listenerIds.add(nodeInfoMap.addListener(entryExpired));

    // This periodic timer will keep the node from expiring as long as the process is running.
    timerId = vertx.setPeriodic(TimeUnit.SECONDS.toMillis(TTL_SECONDS / 2), this::keepNodeAlive);
  }

  private void keepNodeAlive(Long timerId) {
    nodeInfoMap.updateEntryExpirationAsync(
        nodeId, TTL_SECONDS, TimeUnit.SECONDS, 0, TimeUnit.MILLISECONDS);
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
    nodeInfoMap.fastPut(nodeId, nodeInfo, TTL_SECONDS, TimeUnit.SECONDS);
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
    listenerIds.forEach(nodeInfoMap::removeListener);
    vertx.cancelTimer(timerId);
  }
}
