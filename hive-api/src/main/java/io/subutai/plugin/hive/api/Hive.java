package io.subutai.plugin.hive.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Hive extends ApiBase<HiveConfig>
{

    UUID addNode( String hiveClusterName, String hostname );

    UUID statusCheck( String hiveClusterName, String hostname );

    UUID startNode( String hiveClusterName, String hostname );

    UUID stopNode( String hiveClusterName, String hostname );

    UUID restartNode( String hiveClusterName, String hostname );

    UUID uninstallNode( String hiveClusterName, String hostname );

    boolean isInstalled( String hadoopClusterName, String hostname );

    ClusterSetupStrategy getClusterSetupStrategy( HiveConfig config, TrackerOperation trackerOperation );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    void saveConfig( HiveConfig config ) throws ClusterException;

    void deleteConfig( HiveConfig config ) throws ClusterException;
}
