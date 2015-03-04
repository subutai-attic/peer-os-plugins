/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.zookeeper.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Zookeeper extends ApiBase<ZookeeperClusterConfig>
{

    /**
     * Setup zookeeper cluster with given cluster configuration
     *
     * @param config - cluster configuration object {@link org.safehaus.subutai.plugin.zookeeper.api
     * .ZookeeperClusterConfig}
     */
    public UUID installCluster( ZookeeperClusterConfig config );


    /**
     * Starts zookeeper service on given cluster node
     *
     * @param clusterName cluster name
     * @param lxcHostname container's hostname
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID startNode( String clusterName, String lxcHostname );


    /**
     * Stops zookeeper service on given cluster node
     *
     * @param clusterName cluster name
     * @param lxcHostname container's hostname
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID stopNode( String clusterName, String lxcHostname );


    /**
     * Starts zookeeper service on all nodes on given cluster
     *
     * @param clusterName cluster name
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID stopAllNodes( String clusterName );


    /**
     * Stops zookeeper service on all nodes on given cluster
     *
     * @param clusterName cluster name
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID startAllNodes( String clusterName );


    /**
     * Checks status of zookeeper service on given cluster node
     *
     * @param clusterName cluster name
     * @param lxcHostname container's hostname
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID checkNode( String clusterName, String lxcHostname );


    /**
     * Adds nodes to given cluster
     *
     * @param clusterName cluster name
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID addNode( String clusterName );


    /**
     * Adds given node to given cluster
     *
     * @param clusterName name of cluster
     * @param lxcHostname node hostname to be added to zookeeper cluster
     */
    public UUID addNode( String clusterName, String lxcHostname );


    /**
     * Destroys node on given cluster
     *
     * @param clusterName cluster name
     * @param lxcHostname container's hostname
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID destroyNode( String clusterName, String lxcHostname );


    /**
     * Adds some configuration parameters to given file on zookeeper cluster nodes.
     *
     * @param clusterName cluster name
     * @param fileName file name that property will be injected. (e.g. zoo.cfg)
     * @param propertyName property name (e.g. dataDir )
     * @param propertyValue property value (e.g. /var/zookeeper )
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID addProperty( String clusterName, String fileName, String propertyName, String propertyValue );


    /**
     * Removes some configuration parameters from given file on zookeeper cluster nodes.
     *
     * @param clusterName cluster name
     * @param fileName file name that property will be injected. (e.g. zoo.cfg)
     * @param propertyName property name (e.g. dataDir )
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID removeProperty( String clusterName, String fileName, String propertyName );


    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, ZookeeperClusterConfig config,
                                                         TrackerOperation po );


    /**
     * Returns various commands to operate on zookeeper nodes such as start, stop, status etc.
     *
     * @param commandType {@link org.safehaus.subutai.plugin.zookeeper.api.CommandType}
     *
     * @return command string ( e.g service zookeeper start )
     */
    public String getCommand( CommandType commandType );


    /**
     * @param config zookeeper cluster config object {@link org.safehaus.subutai.plugin.zookeeper.api
     * .ZookeeperClusterConfig}
     *
     * @return uuid that can tracked using {@link org.safehaus.subutai.core.tracker.api.Tracker} class.
     */
    public UUID configureEnvironmentCluster( ZookeeperClusterConfig config );


    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( ZookeeperClusterConfig config ) throws ClusterException;

    public void deleteConfig( final ZookeeperClusterConfig config ) throws ClusterException;
}

