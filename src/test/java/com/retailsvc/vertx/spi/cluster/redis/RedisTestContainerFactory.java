package com.retailsvc.vertx.spi.cluster.redis;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class RedisTestContainerFactory {
  public static GenericContainer<?> newContainer() {
    return new GenericContainer<>(DockerImageName.parse("redis:6-alpine"))
        .withCommand("redis-server", "--save", "''")
        .withExposedPorts(6379);
  }
}
