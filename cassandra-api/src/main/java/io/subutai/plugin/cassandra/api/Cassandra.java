package io.subutai.plugin.cassandra.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Cassandra extends ApiBase<CassandraClusterConfig>
{

    UUID startCluster( String clusterName );

    UUID checkCluster( String clusterName );

    UUID stopCluster( String clusterName );

    UUID startService( String clusterName, String hostname );

    UUID stopService( String clusterName, String hostname );

    UUID statusService( String clusterName, String hostname );

    UUID addNode( String clusterName );

    UUID destroyNode( String clusterName, String hostname );

    UUID removeCluster( String clusterName );

    UUID checkNode( String clusterName, String hostname );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, CassandraClusterConfig config,
                                                         TrackerOperation po );


    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( CassandraClusterConfig config ) throws ClusterException;

    public void deleteConfig( CassandraClusterConfig config ) throws ClusterException;
}