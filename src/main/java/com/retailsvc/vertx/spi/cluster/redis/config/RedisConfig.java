package com.retailsvc.vertx.spi.cluster.redis.config;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Redis cluster manager configuration.
 *
 * @author sasjo
 */
@DataObject
@JsonGen
public class RedisConfig {

  /** The client type. */
  private ClientType type = ClientType.STANDALONE;

  /** Redis key namespace. */
  private String keyNamespace;

  /** Default Redis URL. */
  private final String defaultEndpoint;

  /** Redis endpoints. */
  private List<String> endpoints = new ArrayList<>();

  /** Map configuration. */
  private final List<MapConfig> maps = new ArrayList<>();

  /** Lock configuration. */
  private final List<LockConfig> locks = new ArrayList<>();

  /** Use connection listener. */
  private boolean useConnectionListener;

  /** Create the default configuration from existing environment variables. */
  public RedisConfig() {
    defaultEndpoint = RedisConfigProps.getDefaultEndpoint().toASCIIString();
    keyNamespace = RedisConfigProps.getPropertyValue("redis.key.namespace");
    useConnectionListener =
        Boolean.parseBoolean(RedisConfigProps.getPropertyValue("redis.use.connection.listener"));
  }

  /**
   * Copy constructor.
   *
   * @param other the object to clone
   */
  public RedisConfig(RedisConfig other) {
    defaultEndpoint = other.defaultEndpoint;
    type = other.type;
    keyNamespace = other.keyNamespace;
    endpoints = new ArrayList<>(other.endpoints);
    useConnectionListener = other.useConnectionListener;
    other.maps.stream().map(MapConfig::new).forEach(maps::add);
    other.locks.stream().map(LockConfig::new).forEach(locks::add);
  }

  /**
   * Copy from JSON constructor.
   *
   * @param json source JSON
   */
  public RedisConfig(JsonObject json) {
    this();
    RedisConfigConverter.fromJson(json, this);
  }

  /**
   * Set the key namespace to use as prefix for all keys created in Redis.
   *
   * @param keyNamespace the key namespace
   * @return fluent self
   */
  public RedisConfig setKeyNamespace(String keyNamespace) {
    this.keyNamespace = keyNamespace;
    return this;
  }

  /**
   * Returns the key namespace used as prefix for all keys created in Redis.
   *
   * @return the key namespace
   */
  public String getKeyNamespace() {
    return keyNamespace == null ? "" : keyNamespace;
  }

  /**
   * Set the client type.
   *
   * @param type the client type
   * @return fluent self
   */
  public RedisConfig setClientType(ClientType type) {
    this.type = type;
    return this;
  }

  /**
   * Returns the client type.
   *
   * @return the client type
   */
  public ClientType getClientType() {
    return type == null ? ClientType.STANDALONE : type;
  }

  /**
   * Add a client endpoint.
   *
   * @param redisUrl the endpoint connection URL to add
   * @return fluent self
   * @throws IllegalArgumentException if the given string violates RFC 2396
   */
  public RedisConfig addEndpoint(String redisUrl) {
    try {
      new URI(redisUrl);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Illegal redis URL", e);
    }
    endpoints.add(redisUrl);
    return this;
  }

  /**
   * Get the configured URL endpoints. Depending on the client type, this method will return one or
   * more of the endpoints.
   *
   * @return the configured redis endpoints
   */
  public List<String> getEndpoints() {
    if (endpoints == null || endpoints.isEmpty()) {
      return singletonList(defaultEndpoint);
    }
    if (type == ClientType.STANDALONE) {
      return singletonList(endpoints.get(0));
    }
    return unmodifiableList(endpoints);
  }

  /**
   * Add a map configuration.
   *
   * @param mapConfig the map configuration
   * @return fluent self
   */
  public RedisConfig addMap(MapConfig mapConfig) {
    maps.add(mapConfig);
    return this;
  }

  /**
   * Return all map configurations.
   *
   * @return the map configurations.
   */
  public List<MapConfig> getMaps() {
    return unmodifiableList(maps);
  }

  /**
   * Return all lock configurations.
   *
   * @return the lock configurations.
   */
  public List<LockConfig> getLocks() {
    return unmodifiableList(locks);
  }

  /**
   * Add a lock configuration.
   *
   * @param lockConfig the lock configuration
   * @return fluent self
   */
  public RedisConfig addLock(LockConfig lockConfig) {
    locks.add(lockConfig);
    return this;
  }

  private <T extends KeyConfig<?>> Optional<T> findConfig(List<T> list, String name) {
    if (list == null || list.isEmpty()) {
      return Optional.empty();
    }
    return list.stream().filter(opt -> opt.matches(name)).findFirst();
  }

  /**
   * Get the configuration for a named map.
   *
   * @param name the map name
   * @return the configuration if it exists
   */
  @GenIgnore
  public Optional<MapConfig> getMapConfig(String name) {
    return findConfig(maps, name);
  }

  /**
   * GEt the configuration for a named lock.
   *
   * @param name the lock name
   * @return the configuration if it exists
   */
  @GenIgnore
  public Optional<LockConfig> getLockConfig(String name) {
    return findConfig(locks, name);
  }

  /**
   * Returns the connection listener flag.
   *
   * @return <code>true</code> if a connection listener should be used to track Redis connection
   *     state.
   */
  public boolean isUseConnectionListener() {
    return useConnectionListener;
  }

  /**
   * Set the use connection listener flag. If set, a connection listener will be registered to track
   * the Redis connection state.
   *
   * @param useConnectionListener <code>true</code> to use a connection listener, otherwise <code>
   *     false</code>
   * @return fluent self
   */
  public RedisConfig setUseConnectionListener(boolean useConnectionListener) {
    this.useConnectionListener = useConnectionListener;
    return this;
  }

  /**
   * Converts this object to JSON notation.
   *
   * @return JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    RedisConfigConverter.toJson(this, json);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RedisConfig that = (RedisConfig) o;
    return type == that.type
        && Objects.equals(keyNamespace, that.keyNamespace)
        && defaultEndpoint.equals(that.defaultEndpoint)
        && endpoints.equals(that.endpoints)
        && useConnectionListener == that.useConnectionListener
        && maps.equals(that.maps)
        && locks.equals(that.locks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        type, keyNamespace, defaultEndpoint, endpoints, useConnectionListener, maps, locks);
  }

  @Override
  public String toString() {
    return toJson().encodePrettily();
  }
}
