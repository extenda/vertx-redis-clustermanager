package io.vertx.spi.cluster.redis.config;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.Nullable;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Configuration applied to a named Redis key.
 *
 * @param <T> the type of configuration
 * @author sasjo
 */
abstract class KeyConfig<T extends KeyConfig<T>> {

  /** The key name. */
  private String name;

  /** The key name pattern. */
  private String pattern;

  /** Compiled pattern. */
  @GenIgnore private Pattern regex;

  /** Default constructor. */
  KeyConfig() {}

  /**
   * Create a named key config.
   *
   * @param name key name
   */
  KeyConfig(String name) {
    this.name = name;
  }

  /**
   * Create a pattern matching key config.
   *
   * @param pattern key name pattern
   */
  KeyConfig(Pattern pattern) {
    this.regex = pattern;
    this.pattern = pattern.pattern();
  }

  /**
   * Copy constructor.
   *
   * @param other object to clone
   */
  KeyConfig(KeyConfig<T> other) {
    this.name = other.name;
    this.pattern = other.pattern;
    this.regex = other.regex;
  }

  /**
   * Returns the key name.
   *
   * @return the key name.
   */
  public @Nullable String getName() {
    return name;
  }

  /**
   * Set the key name pattern to use.
   *
   * @param pattern the name pattern
   * @return fluent self
   */
  @SuppressWarnings("unchecked")
  public T setPattern(String pattern) {
    this.pattern = pattern;
    this.regex = Pattern.compile(pattern);
    return (T) this;
  }

  /**
   * Set the key name.
   *
   * @param name the key name
   * @return fluent self
   */
  @SuppressWarnings("unchecked")
  public T setName(String name) {
    this.name = name;
    return (T) this;
  }

  /**
   * Returns the key name pattern.
   *
   * @return the key name pattern
   */
  public @Nullable String getPattern() {
    return pattern;
  }

  public boolean matches(String name) {
    if (this.name == null && this.pattern == null) {
      return false;
    } else if (this.name != null) {
      return this.name.equals(name);
    }
    return this.regex.matcher(name).matches();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyConfig<?> keyConfig = (KeyConfig<?>) o;
    return Objects.equals(name, keyConfig.name) && Objects.equals(pattern, keyConfig.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, pattern);
  }
}
