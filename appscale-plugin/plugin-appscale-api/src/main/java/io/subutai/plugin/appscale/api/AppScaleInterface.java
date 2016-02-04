package io.subutai.plugin.appscale.api;


import java.util.List;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface AppScaleInterface extends ApiBase<AppScaleConfig>
{
    @Override
    public List<AppScaleConfig> getClusters ();


    @Override
    public AppScaleConfig getCluster ( String clusterName );


    @Override
    public UUID installCluster ( AppScaleConfig appScaleConfig );


    UUID uninstallCluster ( AppScaleConfig appScaleConfig );


    UUID startCluster ( String clusterName );


    UUID stopCluster ( String clusterName );


    UUID restartCluster ( String clusterName );


    UUID statusCluster ( String clusterName );


    UUID startService ( String clusterName, String hostName );


    UUID stopService ( String clusterName, String hostName );


    UUID statusService ( String clusterName, String hostName );


    UUID addNode ( String clusterName );


    UUID destroyNode ( String clusterName, String hostName );


    UUID removeCluster ( String clusterName );


    public ClusterSetupStrategy getClusterSetupStrategy ( Environment e, TrackerOperation t, AppScaleConfig ac );


    public void saveCongig ( AppScaleConfig ac );


    public void deleteConfig ( AppScaleConfig ac );


    public AppScaleConfig getConfig ( String clusterName );

}

