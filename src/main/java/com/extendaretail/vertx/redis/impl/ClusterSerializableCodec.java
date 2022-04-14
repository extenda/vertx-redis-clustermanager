package com.extendaretail.vertx.redis.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/** A Redisson codec for {@link ClusterSerializable} shared data. */
public class ClusterSerializableCodec extends BaseCodec {

  public static final Codec INSTANCE = new ClusterSerializableCodec();

  final Decoder<Object> decoder =
      (buf, state) -> {
        int classNameLength = buf.readInt();
        String className = buf.readCharSequence(classNameLength, StandardCharsets.UTF_8).toString();
        try {
          Object object = getClassLoader().loadClass(className).newInstance();
          if (!(object instanceof ClusterSerializable)) {
            throw new IOException(
                className + " does not implement " + ClusterSerializable.class.getName());
          }
          ((ClusterSerializable) object).readFromBuffer(buf.readerIndex(), Buffer.buffer(buf));
          return object;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          throw new IOException("Failed to decode class " + className, e);
        }
      };

  final Encoder encoder =
      in -> {
        if (!(in instanceof ClusterSerializable)) {
          throw new IOException("Unsupported type: " + in.getClass());
        }
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        String className = in.getClass().getName();
        out.writeInt(className.length());
        out.writeCharSequence(className, StandardCharsets.UTF_8);
        ((ClusterSerializable) in).writeToBuffer(Buffer.buffer(out));
        return out;
      };

  public ClusterSerializableCodec() {}

  public ClusterSerializableCodec(ClassLoader classLoader) {
    this();
  }

  public ClusterSerializableCodec(ClassLoader classLoader, ClusterSerializableCodec codec) {
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
