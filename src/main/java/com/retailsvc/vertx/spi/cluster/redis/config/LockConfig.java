package com.retailsvc.vertx.spi.cluster.redis.config;

import java.util.Objects;
import java.util.regex.Pattern;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * Redis configuration for locks and semaphores.
 *
 * @author sasjo
 */
@DataObject(generateConverter = true)
public final class LockConfig extends KeyConfig<LockConfig> {

  /** The lease time. */
  private int leaseTime = -1;

  /** Default constructor. */
  public LockConfig() {}

  /**
   * Create a named lock config.
   *
   * @param name lock name
   */
  public LockConfig(String name) {
    super(name);
  }

  /**
   * Create a pattern matching lock config.
   *
   * @param pattern lock name pattern
   */
  public LockConfig(Pattern pattern) {
    super(pattern);
  }

  /**
   * Copy constructor.
   *
   * @param other object to clone
   */
  public LockConfig(LockConfig other) {
    super(other);
    this.leaseTime = other.leaseTime;
  }

  /**
   * Copy from JSON constructor.
   *
   * @param json source JSON
   */
  public LockConfig(JsonObject json) {
    LockConfigConverter.fromJson(json, this);
  }

  /**
   * Set the lease time for the lock. If set to <code>-1</code> it means there's no lease time
   * specified and the lease will not expire.
   *
   * @param leaseTime lease time in milliseconds
   * @return fluent self
   */
  public LockConfig setLeaseTime(int leaseTime) {
    this.leaseTime = leaseTime;
    return this;
  }

  /**
   * Returns the lease time for the lock. If <code>-1</code> it means there's no lease time
   * specified and the lease will not expire.
   *
   * @return the lease time in milliseconds
   */
  public int getLeaseTime() {
    return leaseTime;
  }

  @Override
  public String toString() {
    return String.format("leaseTime=%d", leaseTime);
  }

  /**
   * Converts this object to JSON notation.
   *
   * @return JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    LockConfigConverter.toJson(this, json);
    return json;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LockConfig that = (LockConfig) o;
    return leaseTime == that.leaseTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), leaseTime);
  }
}
