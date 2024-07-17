package io.vertx.spi.cluster.redis;

import io.vertx.core.Future;
import java.io.Serializable;
import java.util.Optional;

/**
 * A settable container object which may or may not contain a non-null value.
 *
 * @param <T> the type of the message in the container
 * @author Andrei Tulba
 */
public interface Container<T extends Serializable> {

  /**
   * Retrieves an optional containing an element stored in the container or empty if no such exists.
   *
   * @return a non-null optional containing the value
   */
  Optional<T> get();

  /**
   * Set an item to the container
   *
   * @param item
   */
  void set(T item);

  /**
   * Retrieves a future with an optional containing an element stored in the container or empty if
   * no such exists.
   *
   * @return a future item stored in container
   */
  Future<Optional<T>> getAsync();

  /**
   * Sets asynchronously an item to the container
   *
   * @param item
   * @return a future with the result of set operation
   */
  Future<Void> setAsync(T item);
}
