package org.apache.ignite.cache;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

import javax.cache.CacheException;
import java.util.UUID;

/**
 * Exception thrown when cache store creation failed
 */
public class CacheCreationException extends CacheException {

    /* cache failed to be created */
    private String cacheName;

    /* node started cache creation */
    private UUID initiatingNodeId;

    /* filter for cache nodes */
    private IgnitePredicate<ClusterNode> nodeFilter;

    public CacheCreationException(Throwable cause, String cacheName, UUID initiatingNodeId, IgnitePredicate<ClusterNode> nodeFilter) {
        super(cause);
        this.cacheName = cacheName;
        this.initiatingNodeId = initiatingNodeId;
        this.nodeFilter = nodeFilter;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public UUID getInitiatingNodeId() {
        return initiatingNodeId;
    }

    public IgnitePredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }
}
