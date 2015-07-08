package io.subutai.plugin.mysql.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeType;


/**
 * Created by tkila on 5/7/15.
 */
public interface MySQLC extends ApiBase<MySQLClusterConfig>
{


    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, MySQLClusterConfig config,
                                                         TrackerOperation po );

    public void saveConfig( MySQLClusterConfig config ) throws ClusterException;

    public void deleteConfig( MySQLClusterConfig config ) throws ClusterException;

    UUID stopService( String clusterName, ContainerHost containerHost, NodeType nodeType );

    UUID startService( String clusterName, ContainerHost containerHost, NodeType nodeType );

    UUID statusService( String clusterName, ContainerHost containerHost, NodeType nodeType );

    UUID destroyService( String clusterName, ContainerHost containerHost, NodeType nodeType );

    UUID startNode( String clusterName, String containerHost, NodeType nodeType );

    UUID stopNode( String clusterName, String containerHost, NodeType nodeType );

    UUID checkNode( String clusterName, String containerHost, NodeType nodeType );

    UUID destroyNode( String clusterName, String containerHost, NodeType nodeType );

    UUID startCluster( String clusterName );

    public UUID stopCluster( String clusterName );

    UUID destroyCluster( String clusterName );

    UUID addNode( String clusterName, NodeType nodeType );

    UUID installSQLServer( String clusterName, ContainerHost containerHost, NodeType nodeType );
}