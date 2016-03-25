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


    UUID uninstallCluster ( AppScaleConfig appScaleConfig );


    Boolean checkIfContainerInstalled ( AppScaleConfig as );


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


    UUID configureSSH ( AppScaleConfig appScaleConfig );


    public void configureSsh ( AppScaleConfig appScaleConfig );


    public ClusterSetupStrategy getClusterSetupStrategy ( Environment e, TrackerOperation t, AppScaleConfig ac );


    public void saveConfig ( AppScaleConfig ac ) throws ClusterException;


    public void deleteConfig ( AppScaleConfig ac );


    public AppScaleConfig getConfig ( String clusterName );


    public UUID growEnvironment ( AppScaleConfig appScaleConfig );
}

