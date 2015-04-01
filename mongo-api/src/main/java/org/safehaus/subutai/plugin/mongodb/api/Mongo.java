/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


/**
 * Mongo manager interface
 */
public interface Mongo extends ApiBase<MongoClusterConfig>
{

    /**
     * adds node to the specified cluster
     *
     * @param clusterName - name of cluster
     * @param nodeType - type of node to add
     *
     * @return - UUID of operation to track
     */
    public UUID addNode( String clusterName, NodeType nodeType );

    /**
     * destroys node in the specified cluster
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID destroyNode( String clusterName, String lxcHostName, NodeType nodeType );

    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID startNode( String clusterName, String lxcHostName, NodeType nodeType );

    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID stopNode( String clusterName, String lxcHostName );

    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     *
     * @return - UUID of operation to track
     */
    public UUID startAllNodes( String clusterName );

    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     *
     * @return - UUID of operation to track
     */
    public UUID stopAllNodes( String clusterName );

    /**
     * Checks status of the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID checkNode( String clusterName, String lxcHostName, NodeType nodeType );


    /**
     * Returns Mongo cluster setup strategy
     *
     * @param config - mongo cluster configuration
     * @param po - product operation tracker
     *
     * @return - strategy
     */
    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, MongoClusterConfig config,
                                                         TrackerOperation po );


    public MongoClusterConfig newMongoClusterConfigInstance();

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( MongoClusterConfig config ) throws ClusterException;

    /**
     * Deletes cluster config in database
     *
     * @param config config to be deleted
     */
    public void deleteConfig( MongoClusterConfig config ) throws ClusterException;
}
