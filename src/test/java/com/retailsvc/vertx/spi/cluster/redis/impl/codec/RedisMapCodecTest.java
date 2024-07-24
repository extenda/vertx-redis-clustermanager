package com.retailsvc.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.redisson.client.codec.Codec;

class RedisMapCodecTest extends CodecTestBase {

  private final Codec codec = new RedisMapCodec();

  @Test
  void stringValue() {
    assertEquals("test", encodeDecode(codec, "test"));
  }

  @Test
  void nullValue() {
    assertNull(encodeDecode(codec, null));
  }

  @Test
  void booleanValue() {
    assertEquals(true, encodeDecode(codec, true));
  }

  @Test
  void integerValue() {
    assertEquals(10, encodeDecode(codec, 10));
  }

  @Test
  void longValue() {
    assertEquals(10L, encodeDecode(codec, 10L));
  }

  @Test
  void serializableValue() {
    assertEquals(Locale.ENGLISH, encodeDecode(codec, Locale.ENGLISH));
  }

  @Test
  void clusterSerializableValue() {
    NodeInfo info = new NodeInfo("localhost", 8080, new JsonObject().put("version", "1.0.0"));
    NodeInfo decoded = encodeDecode(codec, info);
    assertEquals(info, decoded);
    assertNotSame(info, decoded);
  }

  @Test
  void copyCodec() {
    assertNotSame(codec, copy(codec));
  }

  @Test
  void customClassLoader() throws Exception {
    CustomObjectClassLoader classLoader =
        new CustomObjectClassLoader(ClassLoader.getSystemClassLoader());
    RedisMapCodec codec = new RedisMapCodec(classLoader);
    Class<?> objectClass =
        assertDoesNotThrow(() -> classLoader.loadClass(CustomObjectClassLoader.CUSTOM_OBJECT));

    Object object = objectClass.getDeclaredConstructor().newInstance();
    Object decoded = encodeDecode(codec, object);
    assertNotSame(object, decoded);
    assertEquals(object, decoded);
    assertEquals(classLoader, object.getClass().getClassLoader());
    assertEquals(classLoader, decoded.getClass().getClassLoader());
  }
}
