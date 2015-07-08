package io.subutai.plugin.hipi.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Hipi extends ApiBase<HipiConfig>
{

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( HipiConfig config, TrackerOperation po );

    public void saveConfig( final HipiConfig config ) throws ClusterException;

    public void deleteConfig( final HipiConfig config ) throws ClusterException;
}
