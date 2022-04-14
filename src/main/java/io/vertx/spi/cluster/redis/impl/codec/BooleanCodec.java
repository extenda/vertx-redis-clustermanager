package io.vertx.spi.cluster.redis.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/** A Redisson codec for boolean values. */
public class BooleanCodec extends BaseCodec {

  public static final Codec INSTANCE = new BooleanCodec();

  private final Decoder<Object> decoder = (buf, state) -> buf.readBoolean();

  private final Encoder encoder =
      in -> {
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        out.writeBoolean((Boolean) in);
        return out;
      };

  /** Create a BooleanCodec. */
  public BooleanCodec() {}

  /**
   * Create a BooleanCodec.
   *
   * @param classLoader required by Codec contract
   */
  public BooleanCodec(ClassLoader classLoader) {
    this();
  }

  /**
   * Create a BooleanCodec.
   *
   * @param classLoader required by Codec contract
   * @param codec required by Codec contract
   */
  public BooleanCodec(ClassLoader classLoader, BooleanCodec codec) {
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
