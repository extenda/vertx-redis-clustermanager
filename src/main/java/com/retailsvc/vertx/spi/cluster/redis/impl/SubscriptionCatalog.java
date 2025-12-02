package com.retailsvc.vertx.spi.cluster.redis.impl;

import static java.util.Collections.emptySet;

import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage Vertx Event Bus subscriptions in Redis.
 *
 * <p>Inspired and based upon <code>io.vertx.spi.cluster.hazelcast.impl.SubsMapHelper</code>
 *
 * @author sasjo
 */
public class SubscriptionCatalog {

  private static final Logger log = LoggerFactory.getLogger(SubscriptionCatalog.class);

  private final RSetMultimap<String, RegistrationInfo> subsMap;
  private final NodeSelector nodeSelector;
  private final int listenerId;
  private final RTopic topic;
  private final ConcurrentMap<String, Set<RegistrationInfo>> localSubs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Set<RegistrationInfo>> ownSubs = new ConcurrentHashMap<>();
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  private final Throttling throttling;

  /**
   * Create a new subscription catalog.
   *
   * @param redisson the redisson client
   * @param keyFactory the key factory
   * @param nodeSelector Vertx node selector
   */
  public SubscriptionCatalog(
      RedissonClient redisson, RedisKeyFactory keyFactory, NodeSelector nodeSelector) {
    this.nodeSelector = nodeSelector;
    subsMap = redisson.getSetMultimap(keyFactory.vertx("subs"));
    topic = redisson.getTopic(keyFactory.topic("subs"));
    listenerId = topic.addListener(String.class, this::onMessage);
    throttling = new Throttling(this::getAndUpdate);
  }

  /**
   * Create a new subscription catalog.
   *
   * @param predecessor the previous subscription catalog
   * @param redisson the redisson client
   * @param redisKeyFactory the key factory
   * @param nodeSelector Vertx node selector
   */
  public SubscriptionCatalog(
      SubscriptionCatalog predecessor,
      RedissonClient redisson,
      RedisKeyFactory redisKeyFactory,
      NodeSelector nodeSelector) {
    this(redisson, redisKeyFactory, nodeSelector);
    ownSubs.putAll(predecessor.ownSubs);
    localSubs.putAll(predecessor.localSubs);
  }

  private void onMessage(CharSequence channel, String address) {
    log.trace("Address [{}] updated", address);
    fireRegistrationUpdateEvent(address);
  }

  /**
   * Get the registered information about handlers for a given address.
   *
   * @param address a handler address
   * @return a list of registered information for the given address
   */
  public List<RegistrationInfo> get(String address) {
    Lock lock = readWriteLock.readLock();
    lock.lock();
    try {
      Set<RegistrationInfo> remote = subsMap.getAll(address);
      Set<RegistrationInfo> local = localSubs.getOrDefault(address, emptySet());
      List<RegistrationInfo> result;
      if (!local.isEmpty()) {
        result = new ArrayList<>(local.size() + remote.size());
        result.addAll(local);
      } else {
        result = new ArrayList<>(remote.size());
      }
      result.addAll(remote);
      return result;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Store registration information for the cluster manager.
   *
   * @param address the address to register
   * @param registrationInfo the registration information
   */
  public void put(String address, RegistrationInfo registrationInfo) {
    Lock lock = readWriteLock.readLock();
    lock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.compute(address, (k, v) -> addToSet(registrationInfo, v));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.compute(address, (k, v) -> addToSet(registrationInfo, v));
        subsMap.put(address, registrationInfo);
        topic.publish(address);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the updated registration information and notify the node selector. This method is throttled
   * to not fire too often.
   *
   * @param address the modified address
   * @see Throttling
   */
  private void getAndUpdate(String address) {
    if (nodeSelector.wantsUpdatesFor(address)) {
      List<RegistrationInfo> registrationInfos;
      try {
        registrationInfos = get(address);
      } catch (Exception e) {
        log.trace("A failure occurred while retrieving the updated registrations", e);
        registrationInfos = Collections.emptyList();
      }
      nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, registrationInfos));
    }
  }

  /**
   * Fire a registration update event to the node selector.
   *
   * @param address the modified address
   */
  private void fireRegistrationUpdateEvent(String address) {
    throttling.onEvent(address);
  }

  private Set<RegistrationInfo> addToSet(
      RegistrationInfo registrationInfo, Set<RegistrationInfo> value) {
    Set<RegistrationInfo> newValue = value != null ? value : ConcurrentHashMap.newKeySet();
    newValue.add(registrationInfo);
    return newValue;
  }

  /**
   * Remove a registration from the cluster manager.
   *
   * @param address the address to unregister from
   * @param registrationInfo the registration information to remove
   */
  public void remove(String address, RegistrationInfo registrationInfo) {
    Lock lock = readWriteLock.readLock();
    lock.lock();
    try {
      if (registrationInfo.localOnly()) {
        localSubs.computeIfPresent(address, (k, v) -> removeFromSet(registrationInfo, v));
        fireRegistrationUpdateEvent(address);
      } else {
        ownSubs.computeIfPresent(address, (k, v) -> removeFromSet(registrationInfo, v));
        subsMap.remove(address, registrationInfo);
        topic.publish(address);
      }
    } finally {
      lock.unlock();
    }
  }

  private Set<RegistrationInfo> removeFromSet(
      RegistrationInfo registrationInfo, Set<RegistrationInfo> value) {
    value.remove(registrationInfo);
    return value.isEmpty() ? null : value;
  }

  /**
   * Remove subscriptions for all nodes in the passed set.
   *
   * @param nodeIds a set of nodes for which to remove subscriptions
   */
  public void removeAllForNodes(Set<String> nodeIds) {
    Set<String> updated = new HashSet<>();
    subsMap
        .entries()
        .forEach(
            entry -> {
              if (nodeIds.contains(entry.getValue().nodeId())) {
                subsMap.remove(entry.getKey(), entry.getValue());
                updated.add(entry.getKey());
              }
            });
    updated.forEach(topic::publish);
  }

  /**
   * Remove subscriptions for nodes that are not part of the <code>availableNodeIds</code>
   * collection. Own subscriptions are never removed.
   *
   * <p>Unknown nodes with lingering state can be observed in clusters that has scaled down to one
   * and then crashes. If a new node is not available to receive events when node entries expire in
   * Redis, the subscriptions will remain registered in Redis.
   *
   * @param self the node ID of this process
   * @param availableNodeIds a set of available nodes
   */
  public void removeUnknownSubs(String self, Collection<String> availableNodeIds) {

    // Build set of known nodes (cluster members + self)
    Set<String> knownNodes = new HashSet<>(availableNodeIds);
    knownNodes.add(self);

    // Group stale subs by subscription key (address)
    Map<String, List<RegistrationInfo>> cleanupByKey = new HashMap<>();

    for (Map.Entry<String, RegistrationInfo> entry : subsMap.entries()) {
      RegistrationInfo info = entry.getValue();
      if (!knownNodes.contains(info.nodeId())) {
        cleanupByKey.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(info);
      }
    }

    if (cleanupByKey.isEmpty()) {
      return;
    }

    // Bulk remove all unknown subscriptions
    List<String> updatedKeys = bulkRemoveUnknownSubsByKey(cleanupByKey, subsMap);

    // Notify all updated keys across the cluster nodes
    updatedKeys.forEach(topic::publish);
  }

  /**
   * Bulk remove unknown subscriptions grouped by key (address) to reduce round trips to Redis. If a
   * bulk deletion fails, this method falls back to individual deletion per entry.
   *
   * @param cleanupByKey the stale subscriptions to remove, grouped by key (address)
   * @param subscriptions the subscriptions map in Redis
   * @return the updated keys (addresses)
   */
  static List<String> bulkRemoveUnknownSubsByKey(
      Map<String, List<RegistrationInfo>> cleanupByKey,
      RSetMultimap<String, RegistrationInfo> subscriptions) {
    // Stats
    AtomicInteger totalRemoved = new AtomicInteger();
    AtomicInteger totalFailed = new AtomicInteger();
    Map<String, Integer> removedPerNode = new HashMap<>();

    List<String> updatedKeys = new ArrayList<>();
    for (Map.Entry<String, List<RegistrationInfo>> entry : cleanupByKey.entrySet()) {
      String key = entry.getKey();
      List<RegistrationInfo> staleValues = entry.getValue();
      try {
        RSet<RegistrationInfo> set = subscriptions.get(key);

        // Bulk remove all (single round trip to Redis)
        set.removeAll(staleValues);

        // Mark as updated because stale entries existed
        updatedKeys.add(key);

        // Stats
        for (RegistrationInfo info : staleValues) {
          removedPerNode.merge(info.nodeId(), 1, Integer::sum);
          totalRemoved.incrementAndGet();
        }
      } catch (Exception e) {
        // Fallback to safe per-value removal
        log.warn("Bulk removal failed for key [{}], retrying individually", key, e);

        for (RegistrationInfo info : staleValues) {
          boolean ok = subscriptions.remove(key, info);
          if (ok) {
            removedPerNode.merge(info.nodeId(), 1, Integer::sum);
            totalRemoved.incrementAndGet();
          } else {
            totalFailed.incrementAndGet();
          }
        }

        updatedKeys.add(key);
      }
    }

    // Logging summary
    log.warn(
        "Removed {} lingering subscriptions from {} unknown node(s). Breakdown: {}",
        totalRemoved.get(),
        removedPerNode.size(),
        removedPerNode);

    if (totalFailed.get() > 0) {
      log.warn("Failed to remove {} subscriptions", totalFailed.get());
    }
    return updatedKeys;
  }

  /** Republish subscriptions that belongs to the current node (in which this is executed). */
  public void republishOwnSubs() {
    Lock writeLock = readWriteLock.writeLock();
    writeLock.lock();
    try {
      Set<String> updated = new HashSet<>();
      ownSubs.forEach(
          (address, registrationInfos) ->
              registrationInfos.forEach(
                  registrationInfo -> {
                    subsMap.put(address, registrationInfo);
                    updated.add(address);
                  }));
      updated.forEach(topic::publish);
    } finally {
      writeLock.unlock();
    }
  }

  /** Close the subscription catalog. */
  public void close() {
    topic.removeListener(listenerId);
    throttling.close();
  }
}
