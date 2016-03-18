package io.subutai.plugin.nutch.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Nutch extends ApiBase<NutchConfig>
{
    public UUID destroyNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( NutchConfig config, TrackerOperation po );

    public void saveConfig( final NutchConfig config ) throws ClusterException;

    public void deleteConfig( final NutchConfig config ) throws ClusterException;
}
