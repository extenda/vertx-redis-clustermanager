package io.vertx.spi.cluster.redis.impl.shareddata;

import io.vertx.core.shareddata.Lock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RSemaphore;

/** Lock implementation backed by Redis. */
public class RedisLock implements Lock {

  private final ExecutorService lockReleaseExec;
  private final AtomicBoolean released = new AtomicBoolean();
  private final Runnable releaseLock;

  public RedisLock(RSemaphore semaphore, ExecutorService lockReleaseExec) {
    this.lockReleaseExec = lockReleaseExec;
    this.releaseLock = semaphore::release;
  }

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
