package org.safehaus.subutai.plugin.presto.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;


public interface Presto extends ApiBase<PrestoClusterConfig>
{

    public UUID installCluster( PrestoClusterConfig config, HadoopClusterConfig hadoopConfig );

    public UUID uninstallCluster( PrestoClusterConfig config );

    public UUID addWorkerNode( String clusterName, String lxcHostname );

    public UUID destroyWorkerNode( String clusterName, String lxcHostname );

    //public UUID changeCoordinatorNode( String clusterName, String newMasterHostname );

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

    public ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, PrestoClusterConfig config,
                                                         Environment environment );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( final PrestoClusterConfig config ) throws ClusterException;
}
