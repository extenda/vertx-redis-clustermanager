package io.vertx.spi.cluster.redis.impl.shareddata;

import io.vertx.core.shareddata.Lock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RPermitExpirableSemaphore;

/** Lock implementation backed by Redis. */
public class RedisLock implements Lock {

  private final RPermitExpirableSemaphore semaphore;
  private final ExecutorService lockReleaseExec;
  private final String permitId;
  private final AtomicBoolean released = new AtomicBoolean();

  public RedisLock(
      RPermitExpirableSemaphore semaphore, String permitId, ExecutorService lockReleaseExec) {
    this.semaphore = semaphore;
    this.permitId = permitId;
    this.lockReleaseExec = lockReleaseExec;
  }

  @Override
  public void release() {
    if (released.compareAndSet(false, true)) {
      lockReleaseExec.execute(() -> semaphore.release(permitId));
    }
  }
}
