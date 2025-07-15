package com.retailsvc.vertx.spi.cluster.redis.impl;

import java.util.concurrent.locks.Lock;

/**
 * A try-with-resource lock that will be claimed on creation and released with the resource block.
 *
 * <pre>
 *   Lock lock = new ReentrantLock();
 *   // ...
 *   try (var ignored = CloseableLock.lock(lock)) {
 *     // critical section.
 *   }
 * </pre>
 *
 * @author sasjo
 */
public final class CloseableLock implements AutoCloseable {

  private final Lock lock;

  /**
   * Claim the given <code>lock</code>. The returned resource can be used in a try-with-resource
   * block to automatically release the lock with the resource block.
   *
   * <pre>
   *   try (var ignored = CloseableLock.lock(lock)) {
   *     // critical section.
   *   }
   * </pre>
   *
   * @param lock the lock to claim
   * @return an auto closeable lock.
   * @throws NullPointerException if lock is null
   */
  public static CloseableLock lock(Lock lock) {
    return new CloseableLock(lock);
  }

  private CloseableLock(Lock lock) {
    this.lock = lock;
    this.lock.lock();
  }

  /** Release the lock. */
  @Override
  public void close() {
    lock.unlock();
  }
}
