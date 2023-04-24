package io.vertx.spi.cluster.redis;

import io.vertx.core.Future;

/**
 * Redis PubSub topic with subscription support. A topic can be used to send messages between
 * processes. It's possible to implement similar features as with the Vertx EventBus and can be
 * useful when broadcast behavior is desired or when cluster features are disabled.
 *
 * @param <T> the type of messages in the topic
 */
public interface Topic<T> {
  /**
   * Add a subscription to the topic. The returned subscriber ID can be used to unsubscribe from the
   * topic.
   *
   * @param subscriber the subscriber callback
   * @return a future with the subscriber ID. The future completes when the subscriber is registered
   */
  Future<Integer> subscribe(TopicSubscriber<T> subscriber);

  /**
   * Remove a subscription from the topic. If the subscriber isn't known this becomes a no-op.
   *
   * @param subscriberId the subscriber ID to remove
   * @return a future that completes when the subscriber is unregistered
   */
  Future<Void> unsubscribe(int subscriberId);

  /**
   * Publish a message to the topic. All subscribers from all processes will be notified.
   *
   * @param message the message to publish.
   * @return a future that completes with the number of listeners that received the message
   *     broadcast from Redis.
   */
  Future<Long> publish(T message);
}
