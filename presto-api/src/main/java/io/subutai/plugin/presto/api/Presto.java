package io.subutai.plugin.presto.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Presto extends ApiBase<PrestoClusterConfig>
{

    public UUID uninstallCluster( PrestoClusterConfig config );

    public UUID addWorkerNode( String clusterName, String lxcHostname );

    public UUID destroyWorkerNode( String clusterName, String lxcHostname );

    //public UUID changeCoordinatorNode( String clusterName, String newMasterHostname );

    public UUID startAllNodes( String clusterName );

    public UUID stopAllNodes( String clusterName );

    public UUID checkAllNodes( String clusterName );

    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID startNode( String clusterName, String lxcHostName );

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
     * Checks status of the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    public UUID checkNode( String clusterName, String lxcHostName );

    public ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, PrestoClusterConfig config );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( final PrestoClusterConfig config ) throws ClusterException;

    public void deleteConfig( final PrestoClusterConfig config ) throws ClusterException;
}
