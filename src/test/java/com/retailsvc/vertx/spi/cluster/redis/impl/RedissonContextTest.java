package com.retailsvc.vertx.spi.cluster.redis.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.retailsvc.vertx.spi.cluster.redis.config.ClientType;
import com.retailsvc.vertx.spi.cluster.redis.config.RedisConfig;
import org.junit.jupiter.api.Test;

class RedissonContextTest {

  @Test
  void standalone() {
    RedissonContext context = new RedissonContext(new RedisConfig());
    assertTrue(context.getRedissonConfig().isSingleConfig());
    var servers = context.getRedissonConfig().useSingleServer();
    assertEquals("redis://127.0.0.1:6379", servers.getAddress());
    assertNull(servers.getUsername());
    assertNull(servers.getPassword());
  }

  @Test
  void cluster() {
    RedissonContext context =
        new RedissonContext(
            new RedisConfig()
                .setClientType(ClientType.CLUSTER)
                .addEndpoint("redis://node1:6379")
                .addEndpoint("redis://node2:6379")
                .setUsername("username")
                .setPassword("password"));
    assertTrue(context.getRedissonConfig().isClusterConfig());
    var servers = context.getRedissonConfig().useClusterServers();
    assertThat(servers.getNodeAddresses()).containsOnly("redis://node1:6379", "redis://node2:6379");
    assertEquals("username", servers.getUsername());
    assertEquals("password", servers.getPassword());
  }

  @Test
  void replicated() {
    RedissonContext context =
        new RedissonContext(
            new RedisConfig()
                .setClientType(ClientType.REPLICATED)
                .addEndpoint("redis://node1:6379")
                .addEndpoint("redis://node2:6379")
                .setResponseTimeout(100));
    var servers = context.getRedissonConfig().useReplicatedServers();
    assertNotNull(servers);
    assertThat(servers.getNodeAddresses()).containsOnly("redis://node1:6379", "redis://node2:6379");
    assertNull(servers.getUsername());
    assertNull(servers.getPassword());
    assertEquals(100, servers.getTimeout());
  }
}
