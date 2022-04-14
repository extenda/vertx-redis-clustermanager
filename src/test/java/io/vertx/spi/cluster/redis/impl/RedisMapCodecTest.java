package io.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

class RedisMapCodecTest {

  private Codec codec = RedisMapCodec.INSTANCE;

  @Test
  void stringValue() {
    encodeDecode("test");
  }

  @Test
  void nullValue() {
    encodeDecode(null);
  }

  @Test
  void booleanValue() {
    encodeDecode(true);
  }

  @Test
  void integerValue() {
    encodeDecode(10);
  }

  @Test
  void longValue() {
    encodeDecode(10L);
  }

  @Test
  void serializableValue() {
    encodeDecode(Locale.ENGLISH);
  }

  @Test
  void clusterSerializableValue() {
    encodeDecode(new NodeInfo("localhost", 8080, new JsonObject().put("version", "1.0.0")));
  }

  private void encodeDecode(Object value) {
    ByteBuf buf = assertDoesNotThrow(() -> codec.getValueEncoder().encode(value));
    Object decoded = assertDoesNotThrow(() -> codec.getValueDecoder().decode(buf, new State()));
    assertEquals(value, decoded);
  }
}
