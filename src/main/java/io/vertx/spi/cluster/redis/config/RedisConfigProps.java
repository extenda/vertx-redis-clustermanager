package io.vertx.spi.cluster.redis.config;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to access configuration from system properties that fallback to environment variables.
 *
 * @author sasjo
 */
final class RedisConfigProps {

  private static final Logger log = LoggerFactory.getLogger(RedisConfigProps.class);

  private RedisConfigProps() {
    // Prevent initialization
  }

  /**
   * Read a system property value. If the property isn't found, this method will attempt to read it
   * from the system environment. Property names are transformed to env vars by replacing <code>.
   * </code> with <code>_</code> and converting to uppercase.
   *
   * @param propertyName the property name
   * @return the property value or <code>null</code> if not set
   */
  static String getPropertyValue(String propertyName) {
    String envName = propertyName.replace(".", "_").toUpperCase();
    return System.getProperty(propertyName, System.getenv(envName));
  }

  /**
   * Same as {@link #getPropertyValue(String)}, but returns a default value if property is not set.
   *
   * @param propertyName the property name
   * @param defaultValue the default fallback value
   * @return the property value or the fallback value
   */
  static String getPropertyValue(String propertyName, String defaultValue) {
    String value = getPropertyValue(propertyName);
    return value == null ? defaultValue : value;
  }

  /**
   * Returns the Redis server default endpoint.
   *
   * @return the configured Redis server endpoint.
   */
  static URI getDefaultEndpoint() {
    String scheme = getPropertyValue("redis.connection.scheme", "redis");
    String host = getPropertyValue("redis.connection.host", "127.0.0.1");
    String port = getPropertyValue("redis.connection.port", "6379");

    String defaultAddress = scheme + "://" + host + ":" + port;
    String address = getPropertyValue("redis.connection.address", defaultAddress);

    log.debug("Redis endpoint: [{}]", address);
    return URI.create(address);
  }
}
