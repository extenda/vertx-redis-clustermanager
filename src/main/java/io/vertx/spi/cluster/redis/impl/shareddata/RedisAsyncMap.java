package io.vertx.spi.cluster.redis.impl.shareddata;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.redisson.api.RMapCache;

/**
 * Redis implementation of AsyncMap.
 *
 * @param <K> the map key type
 * @param <V> the map value type
 */
public class RedisAsyncMap<K, V> implements AsyncMap<K, V> {

  private final Vertx vertx;
  private final RMapCache<K, V> map;

  public RedisAsyncMap(Vertx vertx, RMapCache<K, V> map) {
    this.vertx = vertx;
    this.map = map;
  }

  @Override
  public Future<V> get(K k) {
    return Future.fromCompletionStage(map.getAsync(k), vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> put(K k, V v) {
    // putAsync is used because fastPutAsync fails acceptance tests.
    return Future.fromCompletionStage(map.putAsync(k, v), vertx.getOrCreateContext())
        .map(result -> null);
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    // putAsync is used because fastPutAsync fails in acceptance tests.
    return Future.fromCompletionStage(
            map.putAsync(k, v, ttl, MILLISECONDS), vertx.getOrCreateContext())
        .map(result -> null);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    return Future.fromCompletionStage(map.putIfAbsentAsync(k, v), vertx.getOrCreateContext());
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    return Future.fromCompletionStage(
        map.putIfAbsentAsync(k, v, ttl, MILLISECONDS), vertx.getOrCreateContext());
  }

  @Override
  public Future<V> remove(K k) {
    return Future.fromCompletionStage(map.removeAsync(k), vertx.getOrCreateContext());
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return Future.fromCompletionStage(map.removeAsync(k, v), vertx.getOrCreateContext());
  }

  @Override
  public Future<V> replace(K k, V v) {
    return Future.fromCompletionStage(map.replaceAsync(k, v), vertx.getOrCreateContext());
  }

  private CompletionStage<Boolean> updateTimeToLive(K k, long ttl) {
    return map.expireEntryAsync(k, Duration.ofMillis(ttl), Duration.ofMillis(0));
  }

  private <R> CompletionStage<R> succeededWith(R value) {
    return Future.succeededFuture(value).toCompletionStage();
  }

  @Override
  public Future<V> replace(K k, V v, long ttl) {
    return Future.fromCompletionStage(
        map.replaceAsync(k, v)
            .thenCompose(
                result -> {
                  if (result != null) {
                    // Replace was successful, update TTL
                    return updateTimeToLive(k, ttl).thenCompose(b -> succeededWith(result));
                  } else {
                    return succeededWith(null);
                  }
                }),
        vertx.getOrCreateContext());
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    return Future.fromCompletionStage(
        map.replaceAsync(k, oldValue, newValue), vertx.getOrCreateContext());
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue, long ttl) {
    return Future.fromCompletionStage(
        map.replaceAsync(k, oldValue, newValue)
            .thenCompose(
                result -> {
                  if (Boolean.TRUE.equals(result)) {
                    // Replace was successful, update TTL
                    return updateTimeToLive(k, ttl).thenCompose(b -> succeededWith(true));
                  } else {
                    return succeededWith(false);
                  }
                }),
        vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> clear() {
    return Future.fromCompletionStage(map.deleteAsync(), vertx.getOrCreateContext())
        .map(result -> null);
  }

  @Override
  public Future<Integer> size() {
    return Future.fromCompletionStage(map.sizeAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Set<K>> keys() {
    return Future.fromCompletionStage(map.readAllKeySetAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<List<V>> values() {
    return Future.fromCompletionStage(map.readAllValuesAsync(), vertx.getOrCreateContext())
        .map(v -> v instanceof List ? (List<V>) v : new ArrayList<>(v));
  }

  @Override
  public Future<Map<K, V>> entries() {
    return Future.fromCompletionStage(map.readAllMapAsync(), vertx.getOrCreateContext());
  }
}
