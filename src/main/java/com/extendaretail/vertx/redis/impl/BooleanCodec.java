package com.extendaretail.vertx.redis.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/** A Redisson codec for boolean values. */
public class BooleanCodec extends BaseCodec {

  public static final Codec INSTANCE = new BooleanCodec();

  final Decoder<Object> decoder = (buf, state) -> buf.readBoolean();

  final Encoder encoder =
      in -> {
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        out.writeBoolean((Boolean) in);
        return out;
      };

  public BooleanCodec() {}

  public BooleanCodec(ClassLoader classLoader) {
    this();
  }

  public BooleanCodec(ClassLoader classLoader, NullCodec codec) {
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
