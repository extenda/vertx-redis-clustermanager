package io.vertx.spi.cluster.redis.impl.codec;

import org.junit.jupiter.api.Test;

class BooleanCodecTest extends CodecTestBase {
  @Test
  void trueValue() {
    encodeDecode(BooleanCodec.INSTANCE, true);
  }

  @Test
  void falseValue() {
    encodeDecode(BooleanCodec.INSTANCE, false);
  }

  @Test
  void copyCodec() {
    copy(BooleanCodec.INSTANCE);
  }
}
