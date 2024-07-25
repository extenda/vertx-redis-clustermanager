package com.retailsvc.vertx.spi.cluster.redis.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.retailsvc.vertx.spi.cluster.redis.impl.shareddata.RedisLock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RSemaphore;

/**
 * A redis semaphore wrapper that supports lock lease time when configured.
 *
 * @author sasjo
 */
class SemaphoreWrapper {
  private RPermitExpirableSemaphore permitSemaphore;
  private RSemaphore semaphore;
  private int leaseTime;

  SemaphoreWrapper(RSemaphore semaphore) {
    this.semaphore = semaphore;
  }

  SemaphoreWrapper(RPermitExpirableSemaphore semaphore, int leaseTime) {
    this.permitSemaphore = semaphore;
    this.leaseTime = leaseTime;
  }

  /**
   * Try to acquire a redis lock.
   *
   * @param waitTime max wait time in milliseconds
   * @param lockReleaseExec lock release thread
   * @return teh acquired lock or <code>null</code> if unsuccessful
   * @throws InterruptedException if interrupted while waiting for lock
   */
  public RedisLock tryAcquire(long waitTime, ExecutorService lockReleaseExec)
      throws InterruptedException {
    RedisLock lock = null;
    if (semaphore != null) {
      if (semaphore.tryAcquire(Duration.ofMillis(waitTime))) {
        lock = new RedisLock(semaphore, lockReleaseExec);
      }
    } else {
      String permitId = permitSemaphore.tryAcquire(waitTime, leaseTime, MILLISECONDS);
      if (permitId != null) {
        lock = new RedisLock(permitSemaphore, permitId, lockReleaseExec);
      }
    }
    return lock;
  }
}
