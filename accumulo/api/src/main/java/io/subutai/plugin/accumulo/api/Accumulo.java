package io.subutai.plugin.accumulo.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Accumulo extends ApiBase<AccumuloClusterConfig>
{
    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     * @param hostId - id of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * slave
     *
     * @return - UUID of operation to track
     */
    UUID startNode( String clusterName, String hostId, boolean master );

    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     * @param hostId - id of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * slave
     *
     * @return - UUID of operation to track
     */
    UUID stopNode( String clusterName, String hostId, boolean master );

    /**
     * Checks status of the specified node
     *
     * @param clusterName - name of cluster
     * @param hostId - id of node
     *
     * @return - UUID of operation to track
     */
    UUID checkNode( String clusterName, String hostId, boolean master );

    /**
     * Destroy specified node
     *
     * @param clusterName - name of cluster
     * @param hostId - id of node
     *
     * @return - UUID of operation to track
     */
    UUID destroyNode( String clusterName, String hostId );

    /**
     * Save cluster configuration
     *
     * @param config - cluster configuration
     */
    void saveConfig( AccumuloClusterConfig config ) throws ClusterException;

    /**
     * Delete cluster configuration
     *
     * @param config - cluster configuration
     */
    void deleteConfig( final AccumuloClusterConfig config ) throws ClusterException;


    ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, AccumuloClusterConfig clusterConfig,
                                                  Environment environment );
}
