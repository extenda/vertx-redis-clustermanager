package io.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import io.vertx.spi.cluster.redis.RedisClusterManager;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.codec.CustomObjectClassLoader;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.redisson.config.Config;

/** Tests the RedisClusterManager creation using different redisson configuration file */
class RedisClusterManagerInitTest {

  private static final int NETTY_THREADS = 64;

  private final CustomObjectClassLoader classLoader =
      new CustomObjectClassLoader(ClassLoader.getSystemClassLoader());

  @Test
  void testCreateManagerUsingYamlFile() throws URISyntaxException {
    URL url = getClass().getClassLoader().getResource("redisson-config.yaml");
    File redissonConfig = new File(url.toURI());

    try (MockedStatic<Config> mocked = mockStatic(Config.class)) {
      mocked
          .when(() -> Config.fromYAML(redissonConfig))
          .thenReturn(new Config().setNettyThreads(NETTY_THREADS));

      RedisClusterManager clusterManager =
          new RedisClusterManager(new RedisConfig(), classLoader, redissonConfig);

      mocked.verify(() -> Config.fromYAML(redissonConfig), times(1));
      assertEquals(NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
    }
  }

  @Test
  void testCreateManagerUsingConfigUrl() throws URISyntaxException {
    URL configUrl = getClass().getClassLoader().getResource("redisson-config.yaml");

    try (MockedStatic<Config> mocked = mockStatic(Config.class)) {
      mocked
          .when(() -> Config.fromYAML(configUrl))
          .thenReturn(new Config().setNettyThreads(NETTY_THREADS));

      RedisClusterManager clusterManager =
          new RedisClusterManager(new RedisConfig(), classLoader, configUrl);

      mocked.verify(() -> Config.fromYAML(configUrl), times(1));
      assertEquals(NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
    }
  }
}
