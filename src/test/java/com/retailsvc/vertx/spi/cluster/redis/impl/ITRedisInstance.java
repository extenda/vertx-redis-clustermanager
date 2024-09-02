package com.retailsvc.vertx.spi.cluster.redis.impl;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.jayway.awaitility.Duration;
import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManager;
import com.retailsvc.vertx.spi.cluster.redis.RedisDataGrid;
import com.retailsvc.vertx.spi.cluster.redis.RedisInstance;
import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import com.retailsvc.vertx.spi.cluster.redis.Topic;
import com.retailsvc.vertx.spi.cluster.redis.TopicSubscriber;
import com.retailsvc.vertx.spi.cluster.redis.config.LockConfig;
import com.retailsvc.vertx.spi.cluster.redis.config.MapConfig;
import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;

/** Test redis specific behavior and data type configurations. */
@Testcontainers
class ITRedisInstance {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private Vertx vertx;
  private RedisClusterManager clusterManager;
  private RedisConfig config;

  @BeforeEach
  void beforeEach() {
    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    config =
        new RedisConfig()
            .addEndpoint(redisUrl)
            .addMap(new MapConfig("maxSize").setMaxSize(3))
            .addLock(new LockConfig("leaseTime").setLeaseTime(1000));
    clusterManager = new RedisClusterManager(config);
    
    Vertx.clusteredVertx(new VertxOptions().setClusterManager(clusterManager)).onComplete(ar -> vertx = ar.result());
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
  void createFromInterface() {
    assertTrue(RedisInstance.create(vertx).isPresent());
  }

  @Test
  void createEmptyFromInterface() {
    assertFalse(RedisInstance.create(null).isPresent());
  }

  @Test
  void getLock() {
    AtomicInteger claimCount = new AtomicInteger(0);
    redisInstance()
        .getLock("getLock")
        .onSuccess(
            lock -> {
              claimCount.incrementAndGet();
              lock.release();
            });
    await().atMost(2, TimeUnit.SECONDS).until(() -> assertEquals(1, claimCount.get()));
  }

  @Test
  void lockWithoutLeaseTime() {
    AtomicInteger claimCount = new AtomicInteger(0);
    AtomicBoolean lockTimeout = new AtomicBoolean(false);

    // Claim a lock but don't release it.
    vertx
        .sharedData()
        .getLockWithTimeout("forever", 1000)
        .onSuccess(lock -> claimCount.incrementAndGet());
    await().atMost(2, TimeUnit.SECONDS).until(() -> assertEquals(1, claimCount.get()));

    // Try to claim the same lock. It should work after lease time expires.
    vertx.sharedData().getLockWithTimeout("forever", 1000).onFailure(t -> lockTimeout.set(true));

    await().atMost(2, TimeUnit.SECONDS).until(lockTimeout::get);
  }

  @Test
  void lockLeaseTime() {
    AtomicInteger claimCount = new AtomicInteger(0);

    // Claim a lock but don't release it.
    vertx
        .sharedData()
        .getLockWithTimeout("leaseTime", 1000)
        .onSuccess(lock -> claimCount.incrementAndGet());
    await().atMost(2, TimeUnit.SECONDS).until(() -> assertEquals(1, claimCount.get()));

    // Try to claim the same lock. It should work after lease time expires.
    vertx
        .sharedData()
        .getLockWithTimeout("leaseTime", 4000)
        .onSuccess(
            lock -> {
              claimCount.incrementAndGet();
              lock.release();
            });

    await().atMost(2, TimeUnit.SECONDS).until(() -> assertEquals(2, claimCount.get()));
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

  @Test
  void topicWithSubscription() {
    Topic<String> topic = redisInstance().getTopic(String.class, "testTopic");
    List<String> messages = new CopyOnWriteArrayList<>();
    List<Long> receivedBy = new ArrayList<>();
    TopicSubscriber<String> subscriber = messages::add;

    AtomicInteger id = new AtomicInteger();
    AtomicBoolean completed = new AtomicBoolean(false);
    topic
        .subscribe(subscriber)
        .onSuccess(id::set)
        .compose(v -> topic.publish("Hello"))
        .onSuccess(receivedBy::add)
        .compose(v -> topic.publish("World"))
        .onSuccess(receivedBy::add)
        .onComplete(v -> completed.set(true));

    await().atMost(2, TimeUnit.SECONDS).until(completed::get);
    await()
        .atMost(2, TimeUnit.SECONDS)
        .until(() -> assertThat(messages).isNotEmpty().containsOnly("Hello", "World"));

    // Unregister and ensure we're not getting the last message.
    completed.set(false);
    topic
        .unsubscribe(id.get())
        .compose(v -> topic.publish("Void"))
        .onSuccess(receivedBy::add)
        .onComplete(v -> completed.set(true));

    await().atMost(2, TimeUnit.SECONDS).until(completed::get);
    assertThat(receivedBy).isEqualTo(asList(1L, 1L, 0L));
    assertThat(messages).doesNotContain("Void");
  }

  @Test
  void shutdownDataGrid() {
    RedisDataGrid dataGrid = RedisDataGrid.create(vertx, config);
    Counter c = assertDoesNotThrow(() -> dataGrid.getCounter("test"));

    assertDoesNotThrow(() -> c.incrementAndGet().toCompletionStage().toCompletableFuture().get());

    assertDoesNotThrow(dataGrid::shutdown);
    await("Graceful shutdown").timeout(Duration.ONE_SECOND);

    ExecutionException ex =
        assertThrows(
            ExecutionException.class,
            () -> c.get().toCompletionStage().toCompletableFuture().get());
    assertThat(ex).hasMessageContaining("Redisson is shutdown");
  }
}
