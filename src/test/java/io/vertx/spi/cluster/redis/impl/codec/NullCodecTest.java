package io.vertx.spi.cluster.redis.impl.codec;

import org.junit.jupiter.api.Test;

class NullCodecTest extends CodecTestBase {
  @Test
  void nullValue() {
    encodeDecode(NullCodec.INSTANCE, null);
  }

  @Test
  void copyCodec() {
    copy(NullCodec.INSTANCE);
  }
}
