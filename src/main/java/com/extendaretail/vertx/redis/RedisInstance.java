package com.extendaretail.vertx.redis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Low-level access to distributed data types backed by Redis. Prefer Vertx shared data types over
 * using this API.
 */
public interface RedisInstance {

  /**
   * Create a distributed lock backed by Redis.
   *
   * @param name the lock name
   * @return a distributed lock.
   */
  DistributedLock getLock(String name);

  /** A distributed lock with lease time support. */
  interface DistributedLock extends Lock {

    /**
     * Acquires the lock with defined <code>leaseTime</code>. Waits if necessary until lock became
     * available.
     *
     * <p>Lock will be released automatically after defined <code>leaseTime</code> interval.
     *
     * @param leaseTime the maximum time to hold the lock after it's acquisition, if it hasn't
     *     already been released by invoking <code>unlock</code>. If leaseTime is -1, hold the lock
     *     until explicitly unlocked.
     * @param unit the time unit
     */
    void lock(long leaseTime, TimeUnit unit);

    /**
     * Tries to acquire the lock with defined <code>leaseTime</code>. Waits up to defined <code>
     * waitTime</code> if necessary until the lock became available.
     *
     * <p>Lock will be released automatically after defined <code>leaseTime</code> interval.
     *
     * @param waitTime the maximum time to acquire the lock
     * @param leaseTime lease time
     * @param unit time unit
     * @return <code>true</code> if lock is successfully acquired, otherwise <code>false</code> if
     *     lock is already set.
     * @throws InterruptedException - if the thread is interrupted
     */
    boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException;
  }
}
