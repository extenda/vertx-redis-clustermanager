package io.vertx.spi.cluster.redis.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import io.vertx.spi.cluster.redis.RedisClusterManager;
import io.vertx.spi.cluster.redis.config.RedisConfig;
import io.vertx.spi.cluster.redis.impl.codec.CustomObjectClassLoader;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.redisson.config.Config;

/** Tests the RedisClusterManager creation using different redisson configuration file */
class RedisClusterManagerInitTest {

  private static final int NETTY_THREADS = 64;

  private static final int DEFAULT_NETTY_THREADS = 32;

  private static final String CONFIG_FILE_NAME = "redisson-config.yaml";

  private static final String WONRG_CONFIG_FILE_NAME = "wrong_file_name";

  private final CustomObjectClassLoader classLoader =
      new CustomObjectClassLoader(ClassLoader.getSystemClassLoader());

  @Test
  void testCreateManagerUsingYamlFile() throws URISyntaxException {
    URL url = getClass().getClassLoader().getResource(CONFIG_FILE_NAME);
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
    URL configUrl = getClass().getClassLoader().getResource(CONFIG_FILE_NAME);

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

  @Test
  void testCreateManagerUsingYamlFile_Error() throws URISyntaxException {
    File redissonConfig = new File("Error");

    try (MockedStatic<Config> mocked = mockStatic(Config.class)) {
      mocked.when(() -> Config.fromYAML(redissonConfig)).thenThrow(IOException.class);

      RedisClusterManager clusterManager =
          new RedisClusterManager(new RedisConfig(), classLoader, redissonConfig);

      mocked.verify(() -> Config.fromYAML(redissonConfig), times(1));
      assertEquals(DEFAULT_NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
    }
  }

  @Test
  void testCreateManagerUsingConfigUrl_Error() throws URISyntaxException {
    URL configUrl = getClass().getClassLoader().getResource(CONFIG_FILE_NAME);

    try (MockedStatic<Config> mocked = mockStatic(Config.class)) {
      mocked.when(() -> Config.fromYAML(configUrl)).thenThrow(IOException.class);

      RedisClusterManager clusterManager =
          new RedisClusterManager(new RedisConfig(), classLoader, configUrl);

      mocked.verify(() -> Config.fromYAML(configUrl), times(1));
      assertEquals(DEFAULT_NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
    }
  }

  @Test
  void testCreateManagerUsingConfigUrl_NullConf() throws URISyntaxException {
    URL configUrl = getClass().getClassLoader().getResource(WONRG_CONFIG_FILE_NAME);
    RedisClusterManager clusterManager =
        new RedisClusterManager(new RedisConfig(), classLoader, configUrl);
    assertEquals(DEFAULT_NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
  }

  @Test
  void testCreateManagerUsingYamlFile_NullConf() throws URISyntaxException {
    File redissonConfig = null;
    RedisClusterManager clusterManager =
        new RedisClusterManager(new RedisConfig(), classLoader, redissonConfig);
    assertEquals(DEFAULT_NETTY_THREADS, clusterManager.getRedissonConfig().getNettyThreads());
  }
}
