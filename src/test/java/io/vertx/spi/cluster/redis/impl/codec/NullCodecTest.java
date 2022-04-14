package io.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class NullCodecTest extends CodecTestBase {
  @Test
  void nullValue() {
    assertNull(encodeDecode(NullCodec.INSTANCE, null));
  }

  @Test
  void copyCodec() {
    assertNotSame(NullCodec.INSTANCE, copy(NullCodec.INSTANCE));
  }
}
