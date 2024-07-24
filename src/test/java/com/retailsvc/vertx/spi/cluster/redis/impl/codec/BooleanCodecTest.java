package com.retailsvc.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;

class BooleanCodecTest extends CodecTestBase {
  @Test
  void trueValue() {
    assertEquals(true, encodeDecode(BooleanCodec.INSTANCE, true));
  }

  @Test
  void falseValue() {
    assertEquals(false, encodeDecode(BooleanCodec.INSTANCE, false));
  }

  @Test
  void copyCodec() {
    assertNotSame(BooleanCodec.INSTANCE, copy(BooleanCodec.INSTANCE));
  }
}
