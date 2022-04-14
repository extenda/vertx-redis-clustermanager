package com.extendaretail.vertx.redis.impl;

/** Create keys for Redis objects. */
public class RedisKeyFactory {

  public static final RedisKeyFactory INSTANCE = new RedisKeyFactory();

  private static final String VERTX = "__vertx";
  private static final String DELIMITER = ":";

  private final String namespace;
  private final boolean hasNamespace;

  /**
   * Create a key factory with the default namespace. The default namespace is loaded from the
   * <code>redis.keyNamespace</code> system property or the <code>REDIS_KEY_NAMESPACE</code> env
   * var.
   */
  public RedisKeyFactory() {
    this(System.getProperty("redis.keyNamespace", System.getenv("REDIS_KEY_NAMESPACE")));
  }

  /**
   * Create a key name factory for a namespace prefix.
   *
   * @param namespace the root namespace.
   */
  public RedisKeyFactory(String namespace) {
    this.namespace = namespace;
    this.hasNamespace = namespace != null && !namespace.isEmpty();
  }

  private String build(String... path) {
    String name = String.join(DELIMITER, path);
    return hasNamespace ? namespace + DELIMITER + name : name;
  }

  public String map(String name) {
    return build(name);
  }

  public String lock(String name) {
    return build(VERTX, "locks", name);
  }

  public String counter(String name) {
    return build(VERTX, "counters", name);
  }

  String topic(String name) {
    return build(VERTX, "topics", name);
  }

  String vertx(String name) {
    return build(VERTX, name);
  }
}
