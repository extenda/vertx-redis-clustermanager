package com.extendaretail.vertx.redis;

import io.vertx.core.shareddata.ClusteredSharedCounterTest;
import io.vertx.core.spi.cluster.ClusterManager;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredSharedCounter extends ClusteredSharedCounterTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
