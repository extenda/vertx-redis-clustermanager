package com.retailsvc.vertx.spi.cluster.redis.impl.codec;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Isolated class loader to load the <code>CustomObject.class</code> file. Used to test custom class
 * loaders during encoding and decoding of data.
 */
public class CustomObjectClassLoader extends ClassLoader {

  public static final String CUSTOM_OBJECT = "CustomObject";

  public CustomObjectClassLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (CUSTOM_OBJECT.equals(name)) {
      try {
        byte[] classBytes =
            Files.readAllBytes(Paths.get("src", "test", "test-class", CUSTOM_OBJECT + ".class"));
        return defineClass(CUSTOM_OBJECT, classBytes, 0, classBytes.length);
      } catch (Exception e) {
        throw new ClassNotFoundException("Failed to load " + CUSTOM_OBJECT, e);
      }
    }
    return super.loadClass(name, resolve);
  }
}
