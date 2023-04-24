package io.vertx.spi.cluster.redis;

/**
 * A subscriber for a {@link Topic}.
 *
 * @param <T> the type of message
 */
@FunctionalInterface
public interface TopicSubscriber<T> {

  /**
   * Invoked for each message posted to the topic.
   *
   * @param message the message
   */
  void onMessage(T message);
}
