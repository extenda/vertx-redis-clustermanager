package com.retailsvc.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;

class CloseableLockTest {

  @Test
  void tryWithResourceLock() {
    Lock lock = mock(Lock.class);
    try (var ignored = CloseableLock.lock(lock)) {
      verify(lock).lock();
    }
    verify(lock).unlock();
  }

  @Test
  void throwsNullPointerOnNullLock() {
    assertThrows(NullPointerException.class, () -> CloseableLock.lock(null));
  }
}
