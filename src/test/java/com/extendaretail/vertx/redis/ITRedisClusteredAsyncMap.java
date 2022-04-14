package com.extendaretail.vertx.redis;

import io.vertx.core.shareddata.ClusteredAsyncMapTest;
import io.vertx.core.spi.cluster.ClusterManager;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredAsyncMap extends ClusteredAsyncMapTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
