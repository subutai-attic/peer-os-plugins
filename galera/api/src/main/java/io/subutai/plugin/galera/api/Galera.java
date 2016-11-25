package io.subutai.plugin.galera.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface Galera extends ApiBase<GaleraClusterConfig>
{
    ClusterSetupStrategy getClusterSetupStrategy( final Environment environment, final GaleraClusterConfig config,
                                                  final TrackerOperation po );

    UUID startNode( String clusterName, String lxcHostname );

    UUID stopNode( String clusterName, String lxcHostname );

    UUID checkNode( String clusterName, String lxcHostname );

    UUID addNode( String clusterName, String lxcHostname );

    UUID destroyNode( String clusterName, String lxcHostname );
}
