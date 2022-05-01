package io.vertx.spi.cluster.redis.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import java.io.IOException;
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

  private ClassLoader classLoader;

  private enum ValueCodec {
    // Enum is ordered according to codec priority
    STRING(String.class, StringCodec.INSTANCE, 0),
    INTEGER(Integer.class, IntegerCodec.INSTANCE, 1),
    BOOLEAN(Boolean.class, BooleanCodec.INSTANCE, 2),
    VERTX(ClusterSerializable.class, ClusterSerializableCodec.INSTANCE, 3),
    JDK(Serializable.class, new SerializationCodec(), 4),
    NULL(Void.class, NullCodec.INSTANCE, 5),
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

    static Codec forEncoded(ByteBuf buf, ClassLoader classLoader) throws IOException {
      int id = buf.readInt();
      for (ValueCodec codec : ValueCodec.values()) {
        if (codec.id == id) {
          try {
            return BaseCodec.copy(classLoader, codec.codec);
          } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to copy Codec " + codec.codec.getClass().getName(), e);
          }
        }
      }
      throw new IllegalArgumentException("No Codec found for id:" + id);
    }
  }

  private final Decoder<Object> decoder =
      (buf, state) -> {
        Codec codec = ValueCodec.forEncoded(buf, getClassLoader());
        return codec.getValueDecoder().decode(buf, state);
      };

  private final Encoder encoder =
      in -> {
        ValueCodec vc = ValueCodec.forObject(in);
        ByteBuf header = ByteBufAllocator.DEFAULT.buffer().writeInt(vc.id);
        ByteBuf payload = vc.codec.getValueEncoder().encode(in);
        return Unpooled.wrappedBuffer(header, payload);
      };

  /** Create a RedisMapCodec. */
  public RedisMapCodec() {}

  /**
   * Create a RedisMapCodec.
   *
   * @param classLoader required by Codec contract
   */
  public RedisMapCodec(ClassLoader classLoader) {
    this();
    this.classLoader = classLoader;
  }

  /**
   * Create a RedisMapCodec.
   *
   * @param classLoader required by Codec contract
   * @param codec required by Codec contract
   */
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

  @Override
  public ClassLoader getClassLoader() {
    if (classLoader != null) {
      return classLoader;
    }
    return super.getClassLoader();
  }
}
