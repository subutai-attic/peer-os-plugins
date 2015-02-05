/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.accumulo.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Blueprint;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.NodeType;


public interface Accumulo extends ApiBase<AccumuloClusterConfig>
{
    public UUID startCluster( String clusterName );

    public UUID stopCluster( String clusterName );

    public UUID checkNode( String clusterName, String lxcHostname );

    /**
     * Adds specified container to existing cluster. Checks if container already configured in cluster, if not, adds
     * environment container first to hadoop cluster and zookeeper cluster, finally installs accumulo and triggers
     * cluster configuration with new environment container.
     *
     * @param clusterName - user specified clusterName
     * @param lxcHostname - user specified environment container name
     * @param nodeType - new node role
     *
     * @return - trackerOperationViewId
     */
    public UUID addNode( String clusterName, String lxcHostname, NodeType nodeType );


    /**
     * Add node to specified cluster, checks if there is environment containers with no accumulo installed, if there is,
     * installs hadoop, zookeeper, accumulo on existing environment container otherwise creates environment container
     * with hadoopManager, on top of it installs zookeeper, accumulo finally initializes accumulo cluster configuration
     * procedure
     *
     * @param clusterName - user specified accumulo cluster
     * @param nodeType - new node role
     *
     * @return - trackerOperationViewId
     */
    public UUID addNode( String clusterName, NodeType nodeType);

    public UUID destroyNode( String clusterName, String lxcHostname, NodeType nodeType );

    public UUID addProperty( String clusterName, String propertyName, String propertyValue );

    public UUID removeProperty( String clusterName, String propertyName );

}
