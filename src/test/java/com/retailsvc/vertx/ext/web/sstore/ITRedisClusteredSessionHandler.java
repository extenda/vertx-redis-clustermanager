package com.retailsvc.vertx.ext.web.sstore;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.web.it.sstore.ClusteredSessionHandlerTest;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisClusteredSessionHandler extends ClusteredSessionHandlerTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }
}
