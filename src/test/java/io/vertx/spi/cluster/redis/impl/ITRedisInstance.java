package io.vertx.spi.cluster.redis.impl;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.RedisClusterManager;
import io.vertx.spi.cluster.redis.RedisInstance;
import io.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.spi.cluster.redis.config.LockConfig;
import io.vertx.spi.cluster.redis.config.MapConfig;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Test redis specific behavior and data type configurations. */
@Testcontainers
class ITRedisInstance {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private Vertx vertx;
  private RedisClusterManager clusterManager;

  @BeforeEach
  void beforeEach() {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    clusterManager =
        new RedisClusterManager(
            new RedisConfig()
                .addEndpoint(redisUrl)
                .addMap(new MapConfig("maxSize").setMaxSize(3))
                .addLock(new LockConfig("leaseTime").setLeaseTime(1000)));
    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
    Vertx.clusteredVertx(
        options,
        ar -> {
          vertx = ar.result();
        });
    await().until(() -> vertx != null);
  }

  @Test
  void nullRedisInstance() {
    RedisClusterManager mgr = new RedisClusterManager();
    assertFalse(mgr.isActive());
    assertFalse(mgr.getRedisInstance().isPresent());
  }

  @Test
  void getRedisInstance() {
    assertTrue(clusterManager.isActive());
    assertTrue(clusterManager.getRedisInstance().isPresent());
  }

  @Test
  void getRedisInstanceFromInterface() {
    assertTrue(RedisInstance.getInstance(vertx).isPresent());
  }

  @Test
  void getEmptyRedisInstanceFromInterface() {
    assertFalse(RedisInstance.getInstance(null).isPresent());
  }

  @Test
  void lockWithoutLeaseTime() {
    AtomicInteger claimCount = new AtomicInteger(0);
    AtomicBoolean lockTimeout = new AtomicBoolean(false);

    // Claim a lock but don't release it.
    vertx
        .sharedData()
        .getLockWithTimeout("forever", 1000)
        .onSuccess(
            lock -> {
              claimCount.incrementAndGet();
            });
    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 1);

    // Try to claim the same lock. It should work after lease time expires.
    vertx
        .sharedData()
        .getLockWithTimeout("forever", 1000)
        .onFailure(
            t -> {
              lockTimeout.set(true);
            });

    await().atMost(2, TimeUnit.SECONDS).until(lockTimeout::get);
  }

  @Test
  void lockLeaseTime() {
    AtomicInteger claimCount = new AtomicInteger(0);

    // Claim a lock but don't release it.
    vertx
        .sharedData()
        .getLockWithTimeout("leaseTime", 1000)
        .onSuccess(
            lock -> {
              claimCount.incrementAndGet();
            });
    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 1);

    // Try to claim the same lock. It should work after lease time expires.
    vertx
        .sharedData()
        .getLockWithTimeout("leaseTime", 4000)
        .onSuccess(
            lock -> {
              claimCount.incrementAndGet();
              lock.release();
            });

    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 2);
  }

  @Test
  void mapMaxSize() throws Exception {
    AtomicBoolean completed = new AtomicBoolean(false);

    List<String> values1 = new ArrayList<>();
    List<String> values2 = new ArrayList<>();

    AsyncMap<String, String> map =
        vertx
            .sharedData()
            .<String, String>getAsyncMap("maxSize")
            .toCompletionStage()
            .toCompletableFuture()
            .get();

    map.put("1", "1")
        .compose(v -> map.put("2", "2"))
        .compose(v -> map.put("3", "3"))
        .compose(v -> map.values().onSuccess(values1::addAll))
        .compose(v -> map.put("4", "4"))
        .compose(v -> map.put("5", "5"))
        .compose(v -> map.values().onSuccess(values2::addAll))
        .onSuccess(v -> completed.set(true));

    await().atMost(2, TimeUnit.SECONDS).until(completed::get);

    assertThat(values1).hasSameElementsAs(asList("1", "2", "3"));
    assertThat(values2).hasSameElementsAs(asList("3", "4", "5"));
  }

  private RedisInstance redisInstance() {
    return clusterManager.getRedisInstance().orElseThrow(IllegalStateException::new);
  }

  @Test
  void ping() {
    assertTrue(redisInstance().ping());
  }

  @Test
  void blockingQueue() throws InterruptedException {
    BlockingQueue<String> queue = redisInstance().getBlockingQueue("testQueue");
    assertNotNull(queue);
    queue.add("1");
    queue.add("2");

    assertThat(queue.take()).isEqualTo("1");
    assertThat(queue.take()).isEqualTo("2");
  }

  @Test
  void blockingDeque() {
    BlockingDeque<String> deque = redisInstance().getBlockingDeque("testDeque");
    assertNotNull(deque);
    deque.push("1");
    deque.push("2");

    assertThat(deque.pop()).isEqualTo("2");
    assertThat(deque.pop()).isEqualTo("1");
  }
}
