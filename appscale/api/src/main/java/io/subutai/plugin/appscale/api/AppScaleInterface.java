package io.subutai.plugin.appscale.api;


import java.util.List;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public interface AppScaleInterface extends ApiBase<AppScaleConfig>
{
    List<String> getClusterList ( Environment name );

    UUID installCluster( AppScaleConfig config );

    UUID uninstallCluster ( AppScaleConfig appScaleConfig );

    UUID statusCluster ( String clusterName );

    UUID startCluster ( String clusterName );

    UUID stopCluster ( String clusterName );

    UUID cleanCluster ( String clusterName );

    public ClusterSetupStrategy getClusterSetupStrategy ( Environment e, TrackerOperation t, AppScaleConfig ac );


    public void saveConfig ( AppScaleConfig ac ) throws ClusterException;


    public void deleteConfig ( AppScaleConfig ac );


    public AppScaleConfig getConfig ( String clusterName );


//    public UUID growEnvironment ( AppScaleConfig appScaleConfig );


    public UUID oneClickInstall ( AppScaleConfig appScaleConfig );
}

