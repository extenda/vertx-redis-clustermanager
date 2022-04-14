package io.vertx.spi.cluster.redis.impl;

/** Listen to changes in the {@link NodeInfoCatalog}. */
public interface NodeInfoCatalogListener {

  /**
   * Invoked when a member is added to the catalog.
   *
   * @param nodeId the UUID of the added node
   */
  void memberAdded(String nodeId);

  /**
   * Invoked when a member is removed from the catalog.
   *
   * @param nodeId the UUID of the removed node
   */
  void memberRemoved(String nodeId);
}
