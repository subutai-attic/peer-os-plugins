package org.safehaus.subutai.plugin.hive.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Hive extends ApiBase<HiveConfig>
{

    public UUID addNode( String hiveClusterName, String hostname );

    public UUID statusCheck( String hiveClusterName, String hostname );

    public UUID startNode( String hiveClusterName, String hostname );

    public UUID stopNode( String hiveClusterName, String hostname );

    public UUID restartNode( String hiveClusterName, String hostname );

    public UUID uninstallNode( String hiveClusterName, String hostname );

    public boolean isInstalled( String hadoopClusterName, String hostname );

    public ClusterSetupStrategy getClusterSetupStrategy( HiveConfig config, TrackerOperation trackerOperation );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( HiveConfig config ) throws ClusterException;

    public void deleteConfig( HiveConfig config ) throws ClusterException;
}
