package org.safehaus.subutai.plugin.shark.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


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

