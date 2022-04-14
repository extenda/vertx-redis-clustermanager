package io.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.netty.buffer.ByteBuf;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

abstract class CodecTestBase {

  <T> T encodeDecode(Codec codec, T value) {
    ByteBuf buf = assertDoesNotThrow(() -> codec.getValueEncoder().encode(value));
    Object decoded = assertDoesNotThrow(() -> codec.getValueDecoder().decode(buf, new State()));
    return (T) decoded;
  }

  Codec copy(Codec codec) {
    return assertDoesNotThrow(() -> BaseCodec.copy(codec.getClassLoader(), codec));
  }
}
