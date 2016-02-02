package io.subutai.plugin.nutch.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Nutch extends ApiBase<NutchConfig>
{
    public UUID destroyNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( NutchConfig config, TrackerOperation po );

    public void saveConfig( final NutchConfig config ) throws ClusterException;

    public void deleteConfig( final NutchConfig config ) throws ClusterException;
}
