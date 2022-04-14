package io.vertx.spi.cluster.redis.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/** A Redisson codec for null values. */
public class NullCodec extends BaseCodec {

  public static final Codec INSTANCE = new NullCodec();

  private static final Decoder<Object> DECODER = (buf, state) -> null;

  private final Encoder encoder =
      in -> {
        if (in != null) {
          throw new IOException("Unexpected non-null value: " + in);
        }
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        out.writeByte(0);
        return out;
      };

  public NullCodec() {}

  public NullCodec(ClassLoader classLoader) {
    this();
  }

  public NullCodec(ClassLoader classLoader, NullCodec codec) {
    this(classLoader);
  }

  @Override
  public Decoder<Object> getValueDecoder() {
    return DECODER;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
