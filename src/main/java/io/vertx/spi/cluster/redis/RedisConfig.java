package io.vertx.spi.cluster.redis;

import io.vertx.spi.cluster.redis.impl.DefaultRedisConfig;
import java.net.URI;
import java.net.URISyntaxException;

/** Configuration for the Redis server connection. */
public interface RedisConfig {

  /**
   * Return the Redis server connection scheme.
   *
   * @return the connection scheme.
   */
  default String getScheme() {
    return getServerAddress().getScheme();
  }

  /**
   * Returns true if the connection is configured to use a secure communication protocol.
   *
   * @return <code>true</code> if configured with secure connection, otherwise <code>false</code>.
   */
  default boolean isSecureConnection() {
    return "rediss".equals(getScheme());
  }

  /**
   * Return the Redis server host.
   *
   * @return the Redis host.
   */
  default String getHost() {
    return getServerAddress().getHost();
  }

  /**
   * Return the Redis server port.
   *
   * @return the Redis server port.
   */
  default int getPort() {
    return getServerAddress().getPort();
  }

  /**
   * Return the Redis server address.
   *
   * @return the Redis server address.
   */
  URI getServerAddress();

  /**
   * Create redis configuration from primitive connection settings.
   *
   * @param scheme the connection scheme
   * @param host the server host
   * @param port the server port
   * @return a configuration with the set values
   * @throws IllegalArgumentException if arguments cannot be used to construct a valid URI
   */
  static RedisConfig withAddress(String scheme, String host, int port) {
    try {
      return new DefaultRedisConfig(new URI(scheme, null, host, port, null, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to create valid URI", e);
    }
  }

  /**
   * Create redis configuration from a server address.
   *
   * @param address the Redis server address
   * @return a configuration with the address.
   * @throws IllegalArgumentException if the given address violates RFC 239
   */
  static RedisConfig withAddress(String address) {
    if (address == null || address.trim().isEmpty()) {
      throw new IllegalArgumentException("address is blank");
    }
    return new DefaultRedisConfig(URI.create(address));
  }

  /**
   * Create redis configuration from a server address.
   *
   * @param address the Redis server address
   * @return a configuration with the address.
   */
  static RedisConfig withAddress(URI address) {
    return new DefaultRedisConfig(address);
  }

  /**
   * Create redis configuration from system properties.
   *
   * @return a configuration with the configured address.
   */
  static RedisConfig withDefaults() {
    return new DefaultRedisConfig();
  }
}
