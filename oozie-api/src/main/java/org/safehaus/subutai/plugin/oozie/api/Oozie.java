package org.safehaus.subutai.plugin.oozie.api;


import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;

import java.util.UUID;


/**
 * @author dilshat
 */
public interface Oozie extends ApiBase<OozieClusterConfig>
{
    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );


//    UUID startServer(OozieClusterConfig config);
//
//    UUID stopServer(OozieClusterConfig config);
//
//    UUID checkServerStatus(OozieClusterConfig config);

    public ClusterSetupStrategy getClusterSetupStrategy(TrackerOperation po, OozieClusterConfig config);

    public EnvironmentBlueprint getDefaultEnvironmentBlueprint(OozieClusterConfig config);

//    UUID addNode(String clustername, String lxchostname, String nodetype);

    UUID destroyNode(final String clusterName, final String lxcHostname);
}
