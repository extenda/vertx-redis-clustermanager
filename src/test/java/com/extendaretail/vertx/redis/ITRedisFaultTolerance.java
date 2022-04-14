package com.extendaretail.vertx.redis;

import io.vertx.core.eventbus.FaultToleranceTest;
import io.vertx.core.spi.cluster.ClusterManager;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisFaultTolerance extends FaultToleranceTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }

  // TODO How will the spawned processes pick up the redis cluster manager? Do we need the service
  // loader file classpath?

  @Override
  protected void afterNodesKilled() throws Exception {
    super.afterNodesKilled();
    // Additional wait to make sure all nodes noticed the shutdowns

    // TODO Can we read this from a variable? Our TTL for nodes is 30s right now.
    Thread.sleep(45_000);
  }
}
