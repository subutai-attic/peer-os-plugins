package org.safehaus.subutai.plugin.elasticsearch.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Elasticsearch extends ApiBase<ElasticsearchClusterConfiguration>
{

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID removeCluster( String clusterName );

    ClusterSetupStrategy getClusterSetupStrategy( ElasticsearchClusterConfiguration elasticsearchClusterConfiguration,
                                                  TrackerOperation po );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;

    public void deleteConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;
}
