/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.accumulo.api;


import java.util.UUID;

import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeType;


public interface Accumulo extends ApiBase<AccumuloClusterConfig>
{
    /**
     * Starts accumulo service in all container hosts within cluster configuration
     *
     * @param clusterName - target cluster name
     *
     * @return - tracker operation id
     */
    public UUID startCluster( String clusterName );

    /**
     * Stops accumulo service in all container hosts within cluster configuration
     *
     * @param clusterName - target cluster name
     *
     * @return - trackerOperation id for tracking logs
     */
    public UUID stopCluster( String clusterName );


    /**
     * check accumulo service status in all container hosts within cluster configuration
     *
     * @param clusterName - target cluster name
     *
     * @return - trackerOperation id for tracking logs
     */
    public UUID checkCluster( String clusterName );


    /**
     * Checks for accumulo service status
     *
     * @param clusterName - cluster name
     * @param lxcHostname - target container host name to view status
     *
     * @return - tracker operation id
     */
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
    public UUID addNode( String clusterName, NodeType nodeType );


    /**
     * Uninstalls accumulo package if target container host is not in masters container hosts group otherwise removes
     * its metadata from slaves list and triggers reconfigure operation on top of cluster
     *
     * @param clusterName - cluster name to modify
     * @param lxcHostname - target host name to uninstall product from
     * @param nodeType - host role in cluster
     *
     * @return - tracker operation id
     */
    public UUID destroyNode( String clusterName, String lxcHostname, NodeType nodeType );


    /**
     * Adds passed properties to accumulo cluster to all container hosts within cluster and restarts cluster master
     * node
     *
     * @param clusterName - cluster name
     * @param propertyName - property key
     * @param propertyValue - property value
     *
     * @return - tracker operation id
     */
    public UUID addProperty( String clusterName, String propertyName, String propertyValue );

    /**
     * Removes property from accumulo cluster over all container hosts within cluster and restarts cluster master node
     *
     * @param clusterName - cluster name
     * @param propertyName - property key to remove
     *
     * @return - tracker operation id
     */
    public UUID removeProperty( String clusterName, String propertyName );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( AccumuloClusterConfig config ) throws ClusterException;

    public void deleteConfig( AccumuloClusterConfig config ) throws ClusterException;
}
