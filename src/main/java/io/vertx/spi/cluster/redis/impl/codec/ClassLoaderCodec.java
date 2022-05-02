package io.vertx.spi.cluster.redis.impl.codec;

import org.redisson.client.codec.BaseCodec;

/** Class loader aware codec. */
public abstract class ClassLoaderCodec extends BaseCodec {

  private final ClassLoader classLoader;

  /** Create a codec with a default class loader. */
  protected ClassLoaderCodec() {
    this(null);
  }

  /**
   * Create a codec with a custom class loader
   *
   * @param classLoader the class loader to use
   */
  protected ClassLoaderCodec(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public final ClassLoader getClassLoader() {
    if (classLoader != null) {
      return classLoader;
    }
    return super.getClassLoader();
  }
}
