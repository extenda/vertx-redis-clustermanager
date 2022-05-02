package io.vertx.core.shareddata;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.redis.RedisClusterManager;
import io.vertx.spi.cluster.redis.RedisConfig;
import io.vertx.spi.cluster.redis.RedisTestContainerFactory;
import io.vertx.spi.cluster.redis.impl.codec.CustomObjectClassLoader;
import io.vertx.test.core.VertxTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class ITCustomClassLoaderAsyncMap extends VertxTestBase {

  @Rule public GenericContainer<?> redis = RedisTestContainerFactory.newContainer();

  private final CustomObjectClassLoader classLoader =
      new CustomObjectClassLoader(ClassLoader.getSystemClassLoader());

  @Override
  protected ClusterManager getClusterManager() {
    return new RedisClusterManager(
        RedisConfig.withAddress("redis", redis.getHost(), redis.getFirstMappedPort()), classLoader);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(2);
  }

  @Test
  public void customObjectNotOnPath() {
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName(CustomObjectClassLoader.CUSTOM_OBJECT));
  }

  @Test
  public void customObjectOnClassLoader() {
    assertDoesNotThrow(
        () -> Class.forName(CustomObjectClassLoader.CUSTOM_OBJECT, false, classLoader));
  }

  @Test
  public void mapPutGetCustomObjectWithClassLoader() throws Exception {
    Class<?> objectClass =
        assertDoesNotThrow(() -> classLoader.loadClass(CustomObjectClassLoader.CUSTOM_OBJECT));
    Object value = objectClass.getDeclaredConstructor().newInstance();
    assertEquals(classLoader, value.getClass().getClassLoader());

    vertices[0]
        .sharedData()
        .<String, Object>getAsyncMap("foo")
        .onSuccess(
            map -> {
              map.put("test", value)
                  .onSuccess(
                      vd -> {
                        vertices[1]
                            .sharedData()
                            .<String, Object>getAsyncMap("foo")
                            .onSuccess(
                                map2 -> {
                                  map2.get("test")
                                      .onSuccess(
                                          res -> {
                                            assertEquals(
                                                classLoader, res.getClass().getClassLoader());
                                            assertEquals(value, res);
                                            testComplete();
                                          })
                                      .onFailure(this::fail);
                                });
                      });
            });
    await();
  }
}
