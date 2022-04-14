package io.vertx.spi.cluster.redis.impl;

import io.vertx.spi.cluster.redis.RedisConfig;
import java.net.URI;
import java.util.Objects;

/** Default implementation of {@link RedisConfig}. */
public class DefaultRedisConfig implements RedisConfig {

  /** Redis server URI. */
  private final URI address;

  public DefaultRedisConfig() {
    address = RedisConfigProps.getServerAddress();
  }

  public DefaultRedisConfig(URI address) {
    if (Objects.isNull(address)) {
      throw new IllegalArgumentException("address is null");
    }
    this.address = address;
  }

  @Override
  public URI getServerAddress() {
    return address;
  }
}
