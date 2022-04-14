package io.vertx.spi.cluster.redis.impl;

import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to access configuration from system properties that fallback to environment variables.
 */
public final class RedisConfigProps {

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
  public static String getPropertyValue(String propertyName) {
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
  public static String getPropertyValue(String propertyName, String defaultValue) {
    String value = getPropertyValue(propertyName);
    return value == null ? defaultValue : value;
  }

  /**
   * Create a Redisson {@link Config} from system properties.
   *
   * @return the redisson configuration.
   */
  public static Config createRedissonConfig() {
    String scheme = getPropertyValue("redis.connection.scheme", "redis");
    String host = getPropertyValue("redis.connection.host", "127.0.0.1");
    String port = getPropertyValue("redis.connection.port", "6379");

    String defaultAddress = scheme + "://" + host + ":" + port;
    String address = getPropertyValue("redis.connection.address", defaultAddress);

    log.debug("Redis address: [{}]", address);

    Config config = new Config();
    config.useSingleServer().setAddress(address);
    return config;
  }
}
