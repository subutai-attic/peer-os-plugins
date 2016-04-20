package io.subutai.plugin.hipi.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Hipi extends ApiBase<HipiConfig>
{

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( HipiConfig config, TrackerOperation po );

    public void saveConfig( final HipiConfig config ) throws ClusterException;

    public void deleteConfig( final HipiConfig config ) throws ClusterException;
}
