package io.vertx.spi.cluster.redis;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ITRedisInstance {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private Vertx vertx;
  private RedisClusterManager clusterManager;

  @BeforeEach
  void beforeEach() {
    clusterManager = RedisClusterManagerTestFactory.newInstance(redis);
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
  void lockLeaseTime() {
    RedisInstance.DistributedLock lock =
        clusterManager
            .getRedisInstance()
            .orElseThrow(IllegalStateException::new)
            .getLock("lockTest");
    lock.lock(2, TimeUnit.SECONDS);
    assertTrue(lock.isLocked());
    await().atMost(3, TimeUnit.SECONDS).until(() -> !lock.isLocked());
  }

  @Test
  void tryLockLeaseTime() throws InterruptedException {
    RedisInstance.DistributedLock lock =
        clusterManager
            .getRedisInstance()
            .orElseThrow(IllegalStateException::new)
            .getLock("tryLocKTest");
    boolean locked = lock.tryLock(1, 2, TimeUnit.SECONDS);
    assertTrue(locked);
    assertTrue(lock.isLocked());
    await().atMost(3, TimeUnit.SECONDS).until(() -> !lock.isLocked());
  }

  @Test
  void ping() {
    RedisInstance redisInstance =
        clusterManager.getRedisInstance().orElseThrow(IllegalStateException::new);
    assertTrue(redisInstance.ping());
  }
}
