package com.retailsvc.vertx.spi.cluster.redis.config;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vertx.core.json.JsonObject;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class RedisConfigTest {

  @Test
  void createDefaults() {
    RedisConfig config = new RedisConfig();
    assertEquals(ClientType.STANDALONE, config.getClientType());
    assertEquals(singletonList("redis://127.0.0.1:6379"), config.getEndpoints());
    assertEquals("", config.getKeyNamespace());
  }

  @Test
  void createFromSystemProperty() {
    try {
      System.setProperty("redis.connection.scheme", "rediss");
      System.setProperty("redis.connection.host", "localhost");
      System.setProperty("redis.connection.port", "8080");
      System.setProperty("redis.key.namespace", "test");

      RedisConfig config = new RedisConfig();
      assertEquals("test", config.getKeyNamespace());
      assertEquals(singletonList("rediss://localhost:8080"), config.getEndpoints());
    } finally {
      System.clearProperty("redis.connection.scheme");
      System.clearProperty("redis.connection.host");
      System.clearProperty("redis.connection.port");
      System.clearProperty("redis.key.namespace");
    }
  }

  @Test
  void createFromSystemPropertyAddress() {
    try {
      System.setProperty("redis.connection.address", "redis://localhost:8080");
      RedisConfig config = new RedisConfig();
      assertEquals(singletonList("redis://localhost:8080"), config.getEndpoints());
    } finally {
      System.clearProperty("redis.connection.address");
    }
  }

  @Test
  void createInvalidEndpoint() {
    RedisConfig config = new RedisConfig();
    assertThrows(IllegalArgumentException.class, () -> config.addEndpoint("@scheme!:invalid"));
  }

  @Test
  void findMapConfigByName() {
    RedisConfig config = new RedisConfig().addMap(new MapConfig("test").setMaxSize(10));
    assertThat(config.getMapConfig("test"))
        .hasValueSatisfying(
            mapConfig -> {
              assertEquals("test", mapConfig.getName());
              assertEquals(10, mapConfig.getMaxSize());
            });
  }

  @Test
  void findMapConfigByPattern() {
    RedisConfig config =
        new RedisConfig().addMap(new MapConfig(Pattern.compile("test:.*")).setMaxSize(1));
    assertThat(config.getMapConfig("test:my-value"))
        .hasValueSatisfying(
            mapConfig -> {
              assertEquals("test:.*", mapConfig.getPattern());
              assertEquals(1, mapConfig.getMaxSize());
            });
  }

  @Test
  void missingMapConfig() {
    assertThat(new RedisConfig().getMapConfig("test")).isEmpty();
  }

  @Test
  void missingLockConfig() {
    assertThat(new RedisConfig().getLockConfig("test")).isEmpty();
  }

  private RedisConfig complexConfig() {
    return new RedisConfig()
        .setClientType(ClientType.CLUSTER)
        .addEndpoint("rediss://localhost:8080")
        .addEndpoint("rediss://localhost:8081")
        .setResponseTimeout(5000)
        .addMap(new MapConfig("test").setMaxSize(10))
        .addMap(
            new MapConfig(Pattern.compile("^prefix\\.*$"))
                .setMaxSize(100)
                .setEvictionMode(EvictionMode.LFU))
        .addLock(new LockConfig("test").setLeaseTime(1000))
        .addLock(new LockConfig(Pattern.compile("lock-.*")).setLeaseTime(-1))
        .setUsername("my-user")
        .setKeyNamespace("test");
  }

  @Test
  void toJson() {
    JsonObject json = complexConfig().toJson();
    assertThat(json.isEmpty()).isFalse();
  }

  @Test
  void fromJson() {
    RedisConfig expected = complexConfig();
    JsonObject json = expected.toJson();
    RedisConfig actual = new RedisConfig(json);
    assertEquals(expected, actual);
    assertEquals(expected.hashCode(), actual.hashCode());
  }

  @Test
  void copy() {
    RedisConfig expected = complexConfig();
    RedisConfig actual = new RedisConfig(expected);
    assertNotSame(expected, actual);
    assertEquals(expected, actual);
    actual.addMap(new MapConfig("test2").setMaxSize(1));
    assertNotEquals(expected, actual);
  }

  @Test
  void configToString() {
    assertDoesNotThrow(() -> complexConfig().toString());
  }

  @Test
  void mapConfigToString() {
    String toString = assertDoesNotThrow(() -> new MapConfig("test").toString());
    assertThat(toString).contains("maxSize=0");
  }

  @Test
  void lockConfigToString() {
    String toString = assertDoesNotThrow(() -> new LockConfig("test").toString());
    assertThat(toString).contains("leaseTime=-1");
  }

  @Test
  void setUsernameAndPassword() {
    RedisConfig config = new RedisConfig().setUsername("test").setPassword("secret");
    assertEquals("test", config.getUsername());
    assertEquals("secret", config.getPassword());
  }

  @Test
  void passwordFromEnv() {
    try {
      System.setProperty("redis.connection.password", "prop-secret");
      RedisConfig config = complexConfig();
      assertEquals("my-user", config.getUsername());
      assertEquals("prop-secret", config.getPassword());
    } finally {
      System.clearProperty("redis.connection.password");
    }
  }

  @Test
  void objectEquals() {
    var complex = complexConfig();
    assertNotEquals(complex, new Object());
    assertNotEquals(complex, new RedisConfig());
    assertEquals(complex, complexConfig());
    assertEquals(complex, complex);
    assertNotEquals(complex, new RedisConfig(complex).setResponseTimeout(20));
    assertNotEquals(complex, new RedisConfig(complex).setUsername(null));
    assertNotEquals(complex, new RedisConfig(complex).setPassword("not null"));
    assertNotEquals(complex, new RedisConfig(complex).setKeyNamespace("test2"));
    assertNotEquals(complex, new RedisConfig(complex).addEndpoint("redis://test"));
    assertNotEquals(complex, new RedisConfig(complex).setClientType(ClientType.REPLICATED));
  }
}
