package com.retailsvc.vertx.spi.cluster.redis.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.retailsvc.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ITSubscriptionCatalog {

  @Container public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private final RedisKeyFactory keyFactory = new RedisKeyFactory("test");
  private NodeSelector nodeSelector;
  private SubscriptionCatalog subsCatalog;

  @BeforeEach
  void beforeEach() {
    nodeSelector = mock(NodeSelector.class);

    String redisUrl = "redis://" + redis.getHost() + ":" + redis.getFirstMappedPort();
    Config config = new Config();
    config.useSingleServer().setAddress(redisUrl);
    RedissonClient redisson = Redisson.create(config);
    subsCatalog = new SubscriptionCatalog(redisson, keyFactory, nodeSelector);
    when(nodeSelector.wantsUpdatesFor(anyString())).thenReturn(true);
  }

  private void putSubs() {
    subsCatalog.put("sub-1", new RegistrationInfo("node1", 1, false));
    subsCatalog.put("sub-2", new RegistrationInfo("node1", 2, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node2", 3, false));
  }

  @Test
  void put() {
    putSubs();
    assertThat(subsCatalog.get("sub-1")).hasSize(2);
    verify(nodeSelector, timeout(100).atLeast(1))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void remove() {
    putSubs();
    subsCatalog.remove("sub-1", new RegistrationInfo("node1", 1, false));
    verify(nodeSelector, timeout(100).atLeast(2))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeLocal() {
    RegistrationInfo reg = new RegistrationInfo("node1", 1, true);
    subsCatalog.put("local-1", reg);
    assertThat(subsCatalog.get("local-1")).containsOnly(reg);

    subsCatalog.remove("local-1", reg);
    assertThat(subsCatalog.get("local-1")).isEmpty();
  }

  @Test
  void republishOwn() {
    subsCatalog.put("local-1", new RegistrationInfo("node1", 1, true));
    subsCatalog.put("remote-1", new RegistrationInfo("node1", 1, false));
    subsCatalog.republishOwnSubs();
    verify(nodeSelector, timeout(100).atLeast(2))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeUnknownSubs() {
    putSubs();
    subsCatalog.put("sub-1", new RegistrationInfo("node3", 4, false));
    subsCatalog.put("sub-1", new RegistrationInfo("node4", 5, false));
    subsCatalog.removeUnknownSubs("node4", asList("node2", "node3"));
    assertThat(subsCatalog.get("sub-1"))
        .containsOnly(
            new RegistrationInfo("node2", 3, false),
            new RegistrationInfo("node3", 4, false),
            new RegistrationInfo("node4", 5, false));
    assertThat(subsCatalog.get("sub-2")).isEmpty();
    verify(nodeSelector, timeout(100).atLeast(1))
        .registrationsUpdated(any(RegistrationUpdateEvent.class));
  }

  @Test
  void removeUnknownSubsNoOp() {
    putSubs();
    subsCatalog.removeUnknownSubs("node3", asList("node1", "node2"));
    assertThat(subsCatalog.get("sub-1")).hasSize(2);
    assertThat(subsCatalog.get("sub-2")).hasSize(1);
  }

  @Test
  void removeForAllNodes() {
    putSubs();
    subsCatalog.removeAllForNodes(singleton("node1"));
    assertThat(subsCatalog.get("sub-1")).doesNotContain(new RegistrationInfo("node1", 1, false));
    assertThat(subsCatalog.get("sub-2")).isEmpty();
  }

  @Test
  void close() {
    assertDoesNotThrow(subsCatalog::close);
  }
}
