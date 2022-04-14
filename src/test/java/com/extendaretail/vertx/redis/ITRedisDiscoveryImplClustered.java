package com.extendaretail.vertx.redis;

import static com.jayway.awaitility.Awaitility.await;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.impl.DiscoveryImpl;
import io.vertx.servicediscovery.impl.DiscoveryImplTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;

public class ITRedisDiscoveryImplClustered extends DiscoveryImplTestBase {
  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  @Before
  public void beforeEach() {
    VertxOptions options =
        new VertxOptions().setClusterManager(RedisClusterManagerTestFactory.newInstance(redis));
    Vertx.clusteredVertx(
        options,
        ar -> {
          vertx = ar.result();
        });
    await().until(() -> vertx != null);
    discovery = new DiscoveryImpl(vertx, new ServiceDiscoveryOptions());
  }
}
