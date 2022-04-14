package com.extendaretail.vertx.redis.impl;

import static io.vertx.core.Future.fromCompletionStage;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Counter;
import org.redisson.api.RAtomicLong;

/** Counter implementation for Redis. */
public class RedisCounter implements Counter {

  private final Vertx vertx;
  private final RAtomicLong counter;

  public RedisCounter(Vertx vertx, RAtomicLong counter) {
    this.vertx = vertx;
    this.counter = counter;
  }

  @Override
  public Future<Long> get() {
    return fromCompletionStage(counter.getAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> incrementAndGet() {
    return fromCompletionStage(counter.incrementAndGetAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> getAndIncrement() {
    return fromCompletionStage(counter.getAndIncrementAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> decrementAndGet() {
    return fromCompletionStage(counter.decrementAndGetAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> addAndGet(long value) {
    return fromCompletionStage(counter.addAndGetAsync(value), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> getAndAdd(long value) {
    return fromCompletionStage(counter.getAndAddAsync(value), vertx.getOrCreateContext());
  }

  @Override
  public Future<Boolean> compareAndSet(long expected, long value) {
    return fromCompletionStage(
        counter.compareAndSetAsync(expected, value), vertx.getOrCreateContext());
  }
}
