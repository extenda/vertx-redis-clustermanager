package com.retailsvc.vertx.servicediscovery.impl;

import static com.jayway.awaitility.Awaitility.await;

import com.retailsvc.vertx.spi.cluster.redis.RedisClusterManagerTestFactory;
import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.impl.DiscoveryImpl;
import io.vertx.servicediscovery.impl.DiscoveryImplTestBase;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.GenericContainer;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITRedisDiscoveryImplClustered extends DiscoveryImplTestBase {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Before
  public void beforeEach() {
    Future<Vertx> future =
        Vertx.builder()
            .withClusterManager(RedisClusterManagerTestFactory.newInstance(redis))
            .buildClustered()
            .onSuccess(v -> vertx = v);

    await().until(future::succeeded);
    await().until(() -> ((VertxInternal) vertx).clusterManager().isActive());

    discovery = new DiscoveryImpl(vertx, new ServiceDiscoveryOptions());
  }
}
