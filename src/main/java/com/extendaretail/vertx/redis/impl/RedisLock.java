package com.extendaretail.vertx.redis.impl;

import io.vertx.core.shareddata.Lock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RSemaphore;

/** Lock implementation backed by Redis. */
public class RedisLock implements Lock {

  private final RSemaphore semaphore;
  private final ExecutorService lockReleaseExec;
  private final AtomicBoolean released = new AtomicBoolean();

  public RedisLock(RSemaphore semaphore, ExecutorService lockReleaseExec) {
    this.semaphore = semaphore;
    this.lockReleaseExec = lockReleaseExec;
  }

  @Override
  public void release() {
    if (released.compareAndSet(false, true)) {
      lockReleaseExec.execute(semaphore::release);
    }
  }
}
