package io.subutai.plugin.spark.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Spark extends ApiBase<SparkClusterConfig>
{

    UUID addSlaveNode( String clusterName, String lxcHostname );

    UUID destroySlaveNode( String clusterName, String lxcHostname );

    UUID changeMasterNode( String clusterName, String newMasterHostname, boolean keepSlave );

    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * slave
     *
     * @return - UUID of operation to track
     */
    UUID startNode( String clusterName, String lxcHostName, boolean master );


    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     *
     * @return - UUID of operation to track
     */
    UUID startCluster( String clusterName );

    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * slave
     *
     * @return - UUID of operation to track
     */
    UUID stopNode( String clusterName, String lxcHostName, boolean master );


    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     *
     * @return - UUID of operation to track
     */
    UUID stopCluster( String clusterName );

    /**
     * Checks the status of the specified cluster
     *
     * @param clusterName - name the cluster
     *
     * @return - UUID of operation to track
     */
    UUID checkCluster( String clusterName );

    /**
     * Checks status of the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     *
     * @return - UUID of operation to track
     */
    UUID checkNode( String clusterName, String lxcHostName, boolean master );


    ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, SparkClusterConfig clusterConfig,
                                                  Environment environment );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    void saveConfig( SparkClusterConfig config ) throws ClusterException;

    void deleteConfig( final SparkClusterConfig config ) throws ClusterException;
}
