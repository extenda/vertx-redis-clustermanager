package com.extendaretail.vertx.redis.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import java.io.Serializable;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.IntegerCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.SerializationCodec;

/** A general purpose Redisson Codec for types supported by shared data maps. */
public class RedisMapCodec extends BaseCodec {

  public static final Codec INSTANCE = new RedisMapCodec();

  private enum ValueCodec {
    // Enum is ordered according to codec priority
    STRING(String.class, StringCodec.INSTANCE, 0),
    INTEGER(Integer.class, IntegerCodec.INSTANCE, 1),
    BOOLEAN(Boolean.class, BooleanCodec.INSTANCE, 2),
    VERTX(ClusterSerializable.class, ClusterSerializableCodec.INSTANCE, 3),
    JDK(Serializable.class, new SerializationCodec(), 4),
    NULL(Void.class, NullCodec.INSTANCE, 5);
    ;

    private final Class<?> type;
    private final Codec codec;
    private final int id;

    ValueCodec(Class<?> type, Codec codec, int id) {
      this.type = type;
      this.codec = codec;
      this.id = id;
    }

    static ValueCodec forObject(Object value) {
      if (value == null) {
        return ValueCodec.NULL;
      }
      for (ValueCodec codec : ValueCodec.values()) {
        if (codec.type.isAssignableFrom(value.getClass())) {
          return codec;
        }
      }
      throw new IllegalArgumentException("No Codec found for type:" + value.getClass().getName());
    }

    static ValueCodec forEncoded(ByteBuf buf) {
      int id = buf.readInt();
      for (ValueCodec codec : ValueCodec.values()) {
        if (codec.id == id) {
          return codec;
        }
      }
      throw new IllegalArgumentException("No Codec found for id:" + id);
    }
  }

  final Decoder<Object> decoder =
      (buf, state) -> {
        ValueCodec vc = ValueCodec.forEncoded(buf);
        return vc.codec.getValueDecoder().decode(buf, state);
      };

  final Encoder encoder =
      in -> {
        ValueCodec vc = ValueCodec.forObject(in);
        ByteBuf header = ByteBufAllocator.DEFAULT.buffer().writeInt(vc.id);
        ByteBuf payload = vc.codec.getValueEncoder().encode(in);
        return Unpooled.wrappedBuffer(header, payload);
      };

  public RedisMapCodec() {}

  public RedisMapCodec(ClassLoader classLoader) {
    this();
  }

  public RedisMapCodec(ClassLoader classLoader, RedisMapCodec codec) {
    this(classLoader);
  }

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
