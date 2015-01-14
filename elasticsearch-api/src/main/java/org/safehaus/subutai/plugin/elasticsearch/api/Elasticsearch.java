package org.safehaus.subutai.plugin.elasticsearch.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Elasticsearch extends ApiBase<ElasticsearchClusterConfiguration>
{

    public UUID startAllNodes( ElasticsearchClusterConfiguration config );

    public UUID checkAllNodes( ElasticsearchClusterConfiguration config );

    public UUID stopAllNodes( ElasticsearchClusterConfiguration config );

    public UUID addNode( String clusterName );

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID uninstallCluster( ElasticsearchClusterConfiguration config );

    ClusterSetupStrategy getClusterSetupStrategy( ElasticsearchClusterConfiguration elasticsearchClusterConfiguration,
                                                  TrackerOperation po );
}
