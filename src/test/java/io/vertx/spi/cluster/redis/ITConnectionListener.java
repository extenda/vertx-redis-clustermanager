package io.vertx.spi.cluster.redis;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.redis.RedisClusterManager.ReconnectListener;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.RedissonContext;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ITConnectionListener {

  @Container public static GenericContainer<?> REDIS = new FixedRedisContainer();

  private RedisClusterManager clusterManager;
  private Vertx vertx;
  private RedissonContext redissonContext;

  private String redisUrl() {
    return "redis://" + REDIS.getHost() + ":" + REDIS.getFirstMappedPort();
  }

  @BeforeEach
  void beforeEach() {
    redissonContext = new RedissonContext(new RedisConfig().addEndpoint(redisUrl()));
    clusterManager = new RedisClusterManager(redissonContext);

    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
    Vertx.clusteredVertx(
        options,
        ar -> {
          vertx = ar.result();
        });
    await().until(() -> vertx != null);
  }

  @Test
  void reconnected() {
    ReconnectListener listener = clusterManager.reconnectListener;
    REDIS.stop();
    await("Redis stopped").until(() -> !REDIS.isRunning());

    await("Disconnected from Redis").atMost(5, TimeUnit.SECONDS).until(listener.disconnected::get);

    Supplier<Boolean> ping =
        () ->
            clusterManager
                .getRedisInstance()
                .map(RedisInstance::ping)
                .orElseThrow(IllegalStateException::new);

    assertFalse(ping.get());

    REDIS.start();
    await("Reconnected with Redis")
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> !listener.disconnected.get());

    assertTrue(ping.get());
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
