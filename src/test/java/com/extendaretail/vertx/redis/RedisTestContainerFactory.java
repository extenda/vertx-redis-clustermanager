package com.extendaretail.vertx.redis;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisTestContainerFactory {
  static GenericContainer<?> newContainer() {
    return new GenericContainer<>(DockerImageName.parse("redis:6-alpine")).withExposedPorts(6379);
  }
}
