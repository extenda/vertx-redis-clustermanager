package com.retailsvc.vertx.spi.cluster.redis;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManager.ReconnectListener;
import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonConnectionListener;
import com.retailsvc.vertx.spi.cluster.redis.impl.RedissonContext;
import io.vertx.core.Vertx;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ITConnectionListener {

  @Container public static GenericContainer<?> redis = new FixedRedisContainer();

  private RedisClusterManager clusterManager;
  private Vertx vertx;
  private RedissonContext redissonContext;

  private String redisUrl() {
    return "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
  }

  private RedissonConnectionListener createVertx(
      boolean useConnectionListener, RedissonConnectionListener listener) {
    redissonContext =
        new RedissonContext(
            new RedisConfig()
                .addEndpoint(redisUrl())
                .setUseConnectionListener(useConnectionListener));
    clusterManager =
        new RedisClusterManager(redissonContext) {
          @Override
          RedissonConnectionListener createConnectionListener() {
            return listener != null ? listener : super.createConnectionListener();
          }
        };

    Vertx.builder().withClusterManager(clusterManager).buildClustered(ar -> vertx = ar.result());
    await().until(() -> vertx != null);
    return clusterManager.reconnectListener;
  }

  @AfterEach
  void afterEach() {
    // Close Vertx to not leak instances between tests.
    vertx.close();
  }

  @RepeatedTest(3)
  void reconnectedOnEnabled() {
    ReconnectListener listener = (ReconnectListener) createVertx(true, null);
    redis.stop();
    await("Redis stopped").until(() -> !redis.isRunning());

    await("Disconnected from Redis").atMost(5, TimeUnit.SECONDS).until(listener.disconnected::get);

    Supplier<Boolean> ping =
        () ->
            clusterManager
                .getRedisInstance()
                .map(RedisInstance::ping)
                .orElseThrow(IllegalStateException::new);

    assertFalse(ping.get());

    redis.start();
    await("Start reconnect with Redis")
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> listener.reconnectStatus.get() != null);
    await("Completed reconnect with Redis")
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> listener.reconnectStatus.get().equals("success"));

    assertTrue(ping.get());
  }

  @Test
  void reconnectOnDisabled() {
    RedissonConnectionListener listener = createVertx(false, spy(RedissonConnectionListener.class));

    redis.stop();
    await("Redis stopped").until(() -> !redis.isRunning());

    redis.start();

    verify(listener, after(500).never()).onConnect();
    verify(listener, after(500).never()).onDisconnect();
  }

  @Test
  void flakyConnection() {
    ReconnectListener listener = (ReconnectListener) createVertx(true, null);
    listener.onConnect();
    listener.onDisconnect();
    listener.onDisconnect();
    listener.onConnect();

    assertNull(listener.reconnectStatus.get());
    assertFalse(listener.reconnectInProgress.get());
    assertFalse(listener.disconnected.get());
  }

  /**
   * A Redis container with a fixed host port. This is required in this test to allow us to start
   * and stop Redis while testing connection listeners.
   */
  public static class FixedRedisContainer extends GenericContainer<FixedRedisContainer> {
    public FixedRedisContainer() {
      super(DockerImageName.parse("redis:6-alpine"));
      withCommand("redis-server", "--save", "''");
      withExposedPorts(6379);
      addFixedExposedPort(6379, 6379);
    }
  }
}
