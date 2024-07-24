package com.retailsvc.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RedisKeyFactoryTest {

  @Test
  void mapWithDefault() {
    assertEquals("test", new RedisKeyFactory("").map("test"));
  }

  @Test
  void mapWithNamespace() {
    assertEquals("namespace:test", new RedisKeyFactory("namespace").map("test"));
  }

  @Test
  void lockWithDefault() {
    assertEquals("__vertx:locks:test", new RedisKeyFactory("").lock("test"));
  }

  @Test
  void lockWithNamespace() {
    assertEquals("namespace:__vertx:locks:test", new RedisKeyFactory("namespace").lock("test"));
  }

  @Test
  void counterWithDefault() {
    assertEquals("__vertx:counters:test", new RedisKeyFactory("").counter("test"));
  }

  @Test
  void counterWithNamespace() {
    assertEquals(
        "namespace:__vertx:counters:test", new RedisKeyFactory("namespace").counter("test"));
  }

  @Test
  void topicWithDefault() {
    assertEquals("__vertx:topics:test", new RedisKeyFactory("").topic("test"));
  }

  @Test
  void topicWithNamespace() {
    assertEquals("namespace:__vertx:topics:test", new RedisKeyFactory("namespace").topic("test"));
  }

  @Test
  void vertxWithDefault() {
    assertEquals("__vertx:test", new RedisKeyFactory("").vertx("test"));
  }

  @Test
  void vertxWithNamespace() {
    assertEquals("namespace:__vertx:test", new RedisKeyFactory("namespace").vertx("test"));
  }
}
