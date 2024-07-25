package com.retailsvc.vertx.spi.cluster.redis.impl.shareddata;

import io.vertx.core.shareddata.Lock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RSemaphore;

/**
 * Lock implementation backed by Redis.
 *
 * @author sasjo
 */
public class RedisLock implements Lock {

  private final ExecutorService lockReleaseExec;
  private final AtomicBoolean released = new AtomicBoolean();
  private final Runnable releaseLock;

  /**
   * Create a Redis distributed lock.
   *
   * @param semaphore the Redisson semaphore that backs the lock
   * @param lockReleaseExec an executor used to release the lock
   */
  public RedisLock(RSemaphore semaphore, ExecutorService lockReleaseExec) {
    this.lockReleaseExec = lockReleaseExec;
    this.releaseLock = semaphore::release;
  }

  /**
   * Create a Redis distributed lock that can expire.
   *
   * @param semaphore the Redisson expirable semaphore that backs the lock
   * @param permitId the internal permit ID
   * @param lockReleaseExec an executor used to release the lock
   */
  public RedisLock(
      RPermitExpirableSemaphore semaphore, String permitId, ExecutorService lockReleaseExec) {
    this.lockReleaseExec = lockReleaseExec;
    this.releaseLock = () -> semaphore.release(permitId);
  }

  @Override
  public void release() {
    if (released.compareAndSet(false, true)) {
      lockReleaseExec.execute(releaseLock);
    }
  }
}
