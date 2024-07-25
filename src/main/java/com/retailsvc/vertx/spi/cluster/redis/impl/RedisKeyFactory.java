package com.retailsvc.vertx.spi.cluster.redis.impl;

/**
 * Create keys for Redis objects.
 *
 * @author sasjo
 */
public class RedisKeyFactory {

  private static final String VERTX = "__vertx";
  private static final String DELIMITER = ":";

  private final String namespace;
  private final boolean hasNamespace;

  /**
   * Create a key name factory for a namespace prefix.
   *
   * @param namespace the root namespace.
   */
  public RedisKeyFactory(String namespace) {
    this.namespace = namespace;
    this.hasNamespace = namespace != null && !namespace.isEmpty();
  }

  String build(String... path) {
    String name = String.join(DELIMITER, path);
    return hasNamespace ? namespace + DELIMITER + name : name;
  }

  String map(String name) {
    return build(name);
  }

  String lock(String name) {
    return build(VERTX, "locks", name);
  }

  String counter(String name) {
    return build(VERTX, "counters", name);
  }

  String topic(String name) {
    return build(VERTX, "topics", name);
  }

  String vertx(String name) {
    return build(VERTX, name);
  }
}
