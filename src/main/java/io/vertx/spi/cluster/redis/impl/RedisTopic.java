package io.vertx.spi.cluster.redis.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.spi.cluster.redis.Topic;
import io.vertx.spi.cluster.redis.TopicSubscriber;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.redisson.api.RTopic;

/**
 * A Redisson topic with subscription support.
 *
 * @param <T> the type of messages in the topic
 */
class RedisTopic<T> implements Topic<T> {

  private final Vertx vertx;
  private final RTopic topic;
  private final Class<T> type;
  private final Map<TopicSubscriber<T>, Integer> subscribers = new ConcurrentHashMap<>();

  RedisTopic(Vertx vertx, Class<T> type, RTopic topic) {
    this.vertx = vertx;
    this.type = type;
    this.topic = topic;
  }

  @Override
  public Future<Void> subscribe(TopicSubscriber<T> subscriber) {
    return Future.fromCompletionStage(
            topic.addListenerAsync(type, (channel, message) -> subscriber.onMessage(message)),
            vertx.getOrCreateContext())
        .onSuccess(id -> subscribers.put(subscriber, id))
        .mapEmpty();
  }

  @Override
  public Future<Void> unsubscribe(TopicSubscriber<T> subscriber) {
    Integer id = subscribers.get(subscriber);
    if (id == null) {
      return Future.succeededFuture();
    }
    return Future.fromCompletionStage(topic.removeListenerAsync(id), vertx.getOrCreateContext());
  }

  @Override
  public Future<Long> publish(T message) {
    return Future.fromCompletionStage(topic.publishAsync(message), vertx.getOrCreateContext());
  }
}
