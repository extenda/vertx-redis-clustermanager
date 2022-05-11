package io.vertx.spi.cluster.redis;

import static com.jayway.awaitility.Awaitility.await;
import static io.vertx.core.Future.succeededFuture;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.spi.cluster.redis.config.LockConfig;
import io.vertx.spi.cluster.redis.config.MapConfig;
import io.vertx.spi.cluster.redis.config.RedisConfig;
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
  void lockWithoutLeaseTime() {
    AtomicInteger claimCount = new AtomicInteger(0);
    AtomicBoolean lockTimeout = new AtomicBoolean(false);

    // Claim a lock but don't release it.
    vertx
        .sharedData()
        .getLockWithTimeout("forever", 1000)
        .onSuccess(
            lock -> {
              assertEquals(1, claimCount.incrementAndGet());
            });
    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 1);

    // Try to claim the same lock. It should work after lease time expires.
    vertx
        .sharedData()
        .getLockWithTimeout("forever", 1000)
        .onSuccess(
            lock -> {
              fail("We don't expect to obtain a lock");
            })
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
              assertEquals(1, claimCount.incrementAndGet());
            });
    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 1);

    // Try to claim the same lock. It should work after lease time expires.
    vertx
        .sharedData()
        .getLockWithTimeout("leaseTime", 4000)
        .onSuccess(
            lock -> {
              assertEquals(2, claimCount.incrementAndGet());
              lock.release();
            });

    await().atMost(2, TimeUnit.SECONDS).until(() -> claimCount.get() == 2);
  }

  @Test
  void mapMaxSize() throws Exception {
    AtomicBoolean completed = new AtomicBoolean(false);
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
        .compose(
            v -> {
              map.values()
                  .onSuccess(
                      values -> {
                        assertThat(values).hasSameElementsAs(asList("1", "2", "3"));
                      });
              return succeededFuture();
            })
        .compose(v -> map.put("4", "4"))
        .compose(v -> map.put("5", "5"))
        .compose(
            v -> {
              map.values()
                  .onSuccess(
                      values -> {
                        assertThat(values).hasSameElementsAs(asList("3", "4", "5"));
                      });
              return succeededFuture();
            })
        .onComplete(
            res -> {
              completed.set(true);
            });
    await().atMost(2, TimeUnit.SECONDS).until(completed::get);
  }

  @Test
  void ping() {
    RedisInstance redisInstance =
        clusterManager.getRedisInstance().orElseThrow(IllegalStateException::new);
    assertTrue(redisInstance.ping());
  }
}
