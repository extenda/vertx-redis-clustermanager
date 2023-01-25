package io.vertx.servicediscovery.impl;

import static com.jayway.awaitility.Awaitility.await;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import io.vertx.spi.cluster.redis.RedisTestContainerFactory;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisDiscoveryImplClustered extends DiscoveryImplTestBase {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Before
  public void beforeEach() {
    VertxOptions options =
        new VertxOptions().setClusterManager(RedisClusterManagerTestFactory.newInstance(redis));
    Promise<Vertx> promise = Promise.promise();
    Vertx.clusteredVertx(options, promise);

    Future<Vertx> future = promise.future().onSuccess(v -> vertx = v);
    await().until(future::succeeded);
    await().until(() -> ((VertxInternal) vertx).getClusterManager().isActive());

    discovery = new DiscoveryImpl(vertx, new ServiceDiscoveryOptions());
  }
}
