package io.subutai.plugin.shark.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Shark extends ApiBase<SharkClusterConfig>
{

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID actualizeMasterIP( String clusterName );

    public ClusterSetupStrategy getClusterSetupStrategy( TrackerOperation po, SharkClusterConfig config,
                                                         Environment environment );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( SharkClusterConfig config ) throws ClusterException;

    public void deleteConfig( final SharkClusterConfig config ) throws ClusterException;
}

