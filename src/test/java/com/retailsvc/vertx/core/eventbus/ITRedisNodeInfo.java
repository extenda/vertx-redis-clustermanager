package com.retailsvc.vertx.core.eventbus;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.tests.eventbus.NodeInfoTest;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisNodeInfo extends NodeInfoTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
