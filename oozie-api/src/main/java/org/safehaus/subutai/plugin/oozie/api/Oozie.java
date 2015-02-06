package org.safehaus.subutai.plugin.oozie.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


/**
 * @author dilshat
 */
public interface Oozie extends ApiBase<OozieClusterConfig>
{
    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( OozieClusterConfig config, TrackerOperation trackerOperation );

    public UUID destroyNode( final String clusterName, final String lxcHostname );

    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( OozieClusterConfig config ) throws ClusterException;

    public void deleteConfig( OozieClusterConfig config ) throws ClusterException;
}
