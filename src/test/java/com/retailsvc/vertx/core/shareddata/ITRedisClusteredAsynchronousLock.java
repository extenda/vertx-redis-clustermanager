package com.retailsvc.vertx.core.shareddata;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.tests.shareddata.ClusteredAsynchronousLockTest;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredAsynchronousLock extends ClusteredAsynchronousLockTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
