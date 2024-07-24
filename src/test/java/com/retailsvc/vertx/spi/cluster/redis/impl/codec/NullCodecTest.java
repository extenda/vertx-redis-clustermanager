package com.retailsvc.vertx.spi.cluster.redis.impl.codec;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class NullCodecTest extends CodecTestBase {
  @Test
  void nullValue() {
    assertNull(encodeDecode(NullCodec.INSTANCE, null));
  }

  @Test
  void nonNullValue() {
    assertThrows(IOException.class, () -> NullCodec.INSTANCE.getValueEncoder().encode("test"));
  }

  @Test
  void copyCodec() {
    assertNotSame(NullCodec.INSTANCE, copy(NullCodec.INSTANCE));
  }
}
