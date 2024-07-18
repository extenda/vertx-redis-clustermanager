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

  private final Vertx vertx;
  private final RBucket<T> bucket;

  public RedisContainer(Vertx vertx, RBucket<T> bucket) {
    this.vertx = vertx;
    this.bucket = bucket;
  }

  @Override
  public Optional<T> get() {
    return Optional.ofNullable(bucket.get());
  }

  @Override
  public void set(T item) {
    bucket.set(item);
  }

  @Override
  public Future<Optional<T>> getAsync() {
    return fromCompletionStage(
        bucket.getAsync().thenApply(Optional::ofNullable), vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> setAsync(T item) {
    return fromCompletionStage(bucket.setAsync(item));
  }
}
