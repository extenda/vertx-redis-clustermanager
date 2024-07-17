package io.vertx.spi.cluster.redis.impl;

import static io.vertx.core.Future.fromCompletionStage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.spi.cluster.redis.Container;
import java.io.Serializable;
import java.util.Optional;
import org.redisson.api.RBucket;

/**
 * A container backed by the Redis bucket
 *
 * @param <T> the type of the message in the container
 * @author Andrei Tulba
 */
public class RedisContainer<T extends Serializable> implements Container<T> {

  private final String name;
  private final Vertx vertx;
  private final RBucket<T> bucket;

  public RedisContainer(String name, Vertx vertx, RBucket<T> bucket) {
    this.name = name;
    this.vertx = vertx;
    this.bucket = bucket;
  }

  @Override
  public Optional<T> get() {
    return Optional.ofNullable(bucket.get());
  }

  @Override
  public void set(T item) {
    if (item == null) {
      bucket.delete();
    } else {
      bucket.set(item);
    }
  }

  @Override
  public Future<Optional<T>> getAsync() {
    return fromCompletionStage(
        bucket.getAsync().thenApply(Optional::ofNullable), vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> setAsync(T item) {
    return item == null
        ? fromCompletionStage(bucket.deleteAsync(), vertx.getOrCreateContext())
            .compose(
                isDeleted ->
                    isDeleted
                        ? Future.succeededFuture()
                        : Future.failedFuture("Failed to clean %s redis container".formatted(name)))
        : fromCompletionStage(bucket.setAsync(item));
  }
}
