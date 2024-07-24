package com.retailsvc.vertx.spi.cluster.redis.config;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Redis configuration for a map.
 *
 * @author sasjo
 */
@DataObject(generateConverter = true)
public final class MapConfig extends KeyConfig<MapConfig> {
  /** The map max size. */
  private int maxSize = 0;

  /** The map eviction mode. */
  private EvictionMode evictionMode = EvictionMode.LRU;

  /**
   * Create a named map config.
   *
   * @param name map name
   */
  public MapConfig(String name) {
    super(name);
  }

  /**
   * Create a pattern matching map config.
   *
   * @param pattern map name pattern
   */
  public MapConfig(Pattern pattern) {
    super(pattern);
  }

  /**
   * Copy constructor.
   *
   * @param other object to clone
   */
  public MapConfig(MapConfig other) {
    super(other);
    this.evictionMode = other.evictionMode;
    this.maxSize = other.maxSize;
  }

  /**
   * Copy from JSON constructor.
   *
   * @param json source JSON
   */
  public MapConfig(JsonObject json) {
    MapConfigConverter.fromJson(json, this);
  }

  /**
   * Set the max size of the map. If set to <code>0</code>, the map is unbounded.
   *
   * @param maxSize the maximum number of keys in the map
   * @return fluent self
   */
  public MapConfig setMaxSize(int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  /**
   * Get the max size of the map. If set to (<code>0</code>), it means the map size is unbound.
   *
   * @return the max number of keys in the map.
   */
  public int getMaxSize() {
    return maxSize;
  }

  /**
   * Set the map eviction mode. This is the algorithm used to evict keys when the max size has been
   * reached.
   *
   * @param evictionMode the eviction mode
   * @return fluent self
   */
  public MapConfig setEvictionMode(EvictionMode evictionMode) {
    this.evictionMode = evictionMode;
    return this;
  }

  /**
   * Returns the map eviction mode.
   *
   * @return the map eviction mode.
   */
  public EvictionMode getEvictionMode() {
    if (evictionMode == null) {
      return EvictionMode.LRU;
    }
    return evictionMode;
  }

  /**
   * Converts this object to JSON notation.
   *
   * @return JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    MapConfigConverter.toJson(this, json);
    return json;
  }

  @Override
  public String toString() {
    return String.format("maxSize=%d, evictionMode=%s", maxSize, getEvictionMode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MapConfig mapConfig = (MapConfig) o;
    return maxSize == mapConfig.maxSize && evictionMode == mapConfig.evictionMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), maxSize, evictionMode);
  }
}
