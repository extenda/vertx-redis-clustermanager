package io.vertx.core.eventbus;

import static java.util.Arrays.asList;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import io.vertx.spi.cluster.redis.RedisTestContainerFactory;
import java.util.List;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisFaultTolerance extends FaultToleranceTest {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Override
  protected ClusterManager getClusterManager() {
    return RedisClusterManagerTestFactory.newInstance(redis);
  }

  @Override
  protected List<String> getExternalNodeSystemProperties() {
    return asList(
        "-Dredis.connection.host=" + redis.getHost(),
        "-Dredis.connection.port=" + redis.getFirstMappedPort());
  }

  @Override
  protected void afterNodesKilled() throws Exception {
    super.afterNodesKilled();
    // Additional wait to make sure all nodes noticed the shutdowns
    Thread.sleep(30_000);
  }
}
