package io.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.spi.cluster.redis.RedisConfig;
import java.net.URI;
import org.junit.jupiter.api.Test;

class DefaultRedisConfigTest {

  @Test
  void withDefaults() {
    RedisConfig config = RedisConfig.withDefaults();
    assertEquals("redis", config.getScheme());
    assertEquals("127.0.0.1", config.getHost());
    assertEquals(6379, config.getPort());
    assertFalse(config.isSecureConnection());
  }

  @Test
  void withAddressString() {
    RedisConfig config = RedisConfig.withAddress("rediss://test:1234");
    assertEquals("rediss", config.getScheme());
    assertEquals("test", config.getHost());
    assertEquals(1234, config.getPort());
    assertTrue(config.isSecureConnection());
  }

  @Test
  void withAddressStringInvalid() {
    assertThrows(IllegalArgumentException.class, () -> RedisConfig.withAddress("|invalid!"));
  }

  @Test
  void withAddressAsEmpty() {
    assertThrows(IllegalArgumentException.class, () -> RedisConfig.withAddress(""));
  }

  @Test
  void withAddressStringAsNull() {
    assertThrows(IllegalArgumentException.class, () -> RedisConfig.withAddress((String) null));
  }

  @Test
  void withAddressURIAsNull() {
    assertThrows(IllegalArgumentException.class, () -> RedisConfig.withAddress((URI) null));
  }
}
