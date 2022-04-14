package io.vertx.spi.cluster.redis.impl.codec;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.redisson.client.codec.Codec;

class RedisMapCodecTest extends CodecTestBase {

  private Codec codec = RedisMapCodec.INSTANCE;

  @Test
  void stringValue() {
    encodeDecode(codec, "test");
  }

  @Test
  void nullValue() {
    encodeDecode(codec, null);
  }

  @Test
  void booleanValue() {
    encodeDecode(codec, true);
  }

  @Test
  void integerValue() {
    encodeDecode(codec, 10);
  }

  @Test
  void longValue() {
    encodeDecode(codec, 10L);
  }

  @Test
  void serializableValue() {
    encodeDecode(codec, Locale.ENGLISH);
  }

  @Test
  void clusterSerializableValue() {
    encodeDecode(codec, new NodeInfo("localhost", 8080, new JsonObject().put("version", "1.0.0")));
  }

  @Test
  void copyCodec() {
    copy(RedisMapCodec.INSTANCE);
  }
}
