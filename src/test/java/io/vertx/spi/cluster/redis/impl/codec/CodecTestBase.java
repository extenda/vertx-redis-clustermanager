package io.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

abstract class CodecTestBase {

  void encodeDecode(Codec codec, Object value) {
    ByteBuf buf = assertDoesNotThrow(() -> codec.getValueEncoder().encode(value));
    Object decoded = assertDoesNotThrow(() -> codec.getValueDecoder().decode(buf, new State()));
    assertEquals(value, decoded);
  }

  void copy(Codec codec) {
    Codec copy = assertDoesNotThrow(() -> BaseCodec.copy(codec.getClassLoader(), codec));
    assertNotSame(codec, copy);
  }
}
