package io.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.redisson.client.codec.Codec;
import org.redisson.client.handler.State;

class ClusterSerializableCodecTest {
  private Codec codec = ClusterSerializableCodec.INSTANCE;
  private NodeInfo info = new NodeInfo("localhost", 8080, new JsonObject().put("version", "1.0.0"));

  @Test
  void encode() {
    ByteBuf buf = assertDoesNotThrow(() -> codec.getValueEncoder().encode(info));
    int length = NodeInfo.class.getName().length();
    assertEquals(length, buf.readInt());
    String encodedClassName = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    assertEquals(NodeInfo.class.getName(), encodedClassName);
  }

  @Test
  void encodeDecode() {
    ByteBuf buf = assertDoesNotThrow(() -> codec.getValueEncoder().encode(info));
    Object decoded = assertDoesNotThrow(() -> codec.getValueDecoder().decode(buf, new State()));
    assertInstanceOf(NodeInfo.class, decoded);
    assertEquals(info, decoded);
  }

  @Test
  void decodeFailsIfMissingClassName() {
    Buffer buffer = Buffer.buffer(ByteBufAllocator.DEFAULT.buffer());
    info.writeToBuffer(buffer);
    assertThrows(
        IOException.class, () -> codec.getValueDecoder().decode(buffer.getByteBuf(), new State()));
  }

  @Test
  void encodeFailsIfNotClusterSerializable() {
    assertThrows(IOException.class, () -> codec.getValueEncoder().encode("Test"));
  }
}
