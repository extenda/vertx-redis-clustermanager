package com.retailsvc.vertx.spi.cluster.redis.impl;

import static com.retailsvc.vertx.spi.cluster.redis.impl.CloseableLock.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Throttle subscription catalog events. This class is based on the throttle implementation for <a
 * href="https://github.com/vert-x3/vertx-hazelcast/blob/master/src/main/java/io/vertx/spi/cluster/hazelcast/impl/Throttling.java">Hazelcast</a>.
 */
class Throttling {

  private enum State {
    NEW {
      @Override
      State pending() {
        return PENDING;
      }

      @Override
      State start() {
        return RUNNING;
      }
    },
    PENDING {
      @Override
      State pending() {
        return this;
      }

      @Override
      State start() {
        return RUNNING;
      }
    },
    RUNNING {
      @Override
      State pending() {
        return RUNNING_PENDING;
      }

      @Override
      State done() {
        return FINISHED;
      }
    },
    RUNNING_PENDING {
      @Override
      State pending() {
        return this;
      }

      @Override
      State done() {
        return FINISHED_PENDING;
      }
    },
    FINISHED {
      @Override
      State pending() {
        return FINISHED_PENDING;
      }

      @Override
      State next() {
        return null;
      }
    },
    FINISHED_PENDING {
      @Override
      State pending() {
        return this;
      }

      @Override
      State next() {
        return NEW;
      }
    };

    State pending() {
      throw new IllegalStateException();
    }

    State start() {
      throw new IllegalStateException();
    }

    State done() {
      throw new IllegalStateException();
    }

    State next() {
      throw new IllegalStateException();
    }
  }

  private static final long SCHEDULE_DELAY = 20;

  private final Consumer<String> action;
  private final ScheduledExecutorService executorService;
  private final ConcurrentMap<String, State> map;

  /**
   * The counter is incremented when a new event is received.
   *
   * <p>It is decremented:
   *
   * <ul>
   *   <li>immediately if the map already contains an entry for the corresponding address, or
   *   <li>when the map entry is removed
   * </ul>
   *
   * When the close method is invoked, the counter is set to -1 and the previous value (N) is
   * stored. A negative counter value prevents new events from being handled. The close method
   * blocks until the counter reaches the value -(1 + N). This allows to stop the throttling
   * gracefully.
   */
  private final AtomicInteger counter;

  private final Lock lock;
  private final Condition condition;

  Throttling(Consumer<String> action) {
    this.action = action;
    this.executorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "vertx-redis-throttling-thread");
              thread.setDaemon(true);
              return thread;
            });
    map = new ConcurrentHashMap<>();
    counter = new AtomicInteger();
    lock = new ReentrantLock();
    condition = lock.newCondition();
  }

  public void onEvent(String address) {
    if (!tryIncrementCounter()) {
      return;
    }
    State curr = map.compute(address, (s, state) -> state == null ? State.NEW : state.pending());
    if (curr == State.NEW) {
      executorService.execute(() -> run(address));
    } else {
      decrementCounter();
    }
  }

  private void run(String address) {
    map.computeIfPresent(address, (s, state) -> state.start());
    try {
      action.accept(address);
    } finally {
      map.computeIfPresent(address, (s, state) -> state.done());
      executorService.schedule(() -> checkState(address), SCHEDULE_DELAY, TimeUnit.MILLISECONDS);
    }
  }

  private void checkState(String address) {
    State curr = map.computeIfPresent(address, (s, state) -> state.next());
    if (curr == State.NEW) {
      run(address);
    } else {
      decrementCounter();
    }
  }

  private boolean tryIncrementCounter() {
    int i;
    do {
      i = counter.get();
      if (i < 0) {
        return false;
      }
    } while (!counter.compareAndSet(i, i + 1));
    return true;
  }

  private void decrementCounter() {
    if (counter.decrementAndGet() < 0) {
      try (var ignored = lock(lock)) {
        condition.signalAll();
      }
    }
  }

  public void close() {
    try (var ignored = lock(lock)) {
      int i = counter.getAndSet(-1);
      if (i == 0) {
        return;
      }
      boolean interrupted = false;
      do {
        try {
          condition.await();
        } catch (InterruptedException e) { // NOSONAR
          interrupted = true;
        }
      } while (counter.get() != -(i + 1));
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
