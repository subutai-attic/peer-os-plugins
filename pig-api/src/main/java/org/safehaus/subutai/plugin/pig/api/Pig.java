package org.safehaus.subutai.plugin.pig.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Pig extends ApiBase<PigConfig>
{
    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID uninstallCluster( PigConfig config );

    public ClusterSetupStrategy getClusterSetupStrategy( PigConfig config, TrackerOperation po );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( PigConfig config ) throws ClusterException;

    public void deleteConfig( PigConfig config ) throws ClusterException;
}
