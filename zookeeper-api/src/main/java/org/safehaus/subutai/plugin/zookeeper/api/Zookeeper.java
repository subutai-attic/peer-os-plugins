/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.zookeeper.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Zookeeper extends ApiBase<ZookeeperClusterConfig>
{

    public UUID installCluster( ZookeeperClusterConfig config );

    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName );

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID addProperty( String clusterName, String fileName, String propertyName, String propertyValue );

    public UUID removeProperty( String clusterName, String fileName, String propertyName );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, ZookeeperClusterConfig config,
                                                         TrackerOperation po );

    public String getCommand( CommandType commandType );


    public UUID configureEnvironmentCluster( ZookeeperClusterConfig config );
}

