package org.safehaus.subutai.plugin.oozie.api;


import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
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

    public ClusterSetupStrategy getClusterSetupStrategy(Environment environment, OozieClusterConfig config, TrackerOperation trackerOperation);

    public EnvironmentBlueprint getDefaultEnvironmentBlueprint(OozieClusterConfig config);

    public UUID destroyNode(final String clusterName, final String lxcHostname);
}
