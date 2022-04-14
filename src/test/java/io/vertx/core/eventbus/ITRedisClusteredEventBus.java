package io.vertx.core.eventbus;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import io.vertx.spi.cluster.redis.RedisTestContainerFactory;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredEventBus extends ClusteredEventBusTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
