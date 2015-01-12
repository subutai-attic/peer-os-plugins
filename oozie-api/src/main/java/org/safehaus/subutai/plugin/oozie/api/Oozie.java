package org.safehaus.subutai.plugin.oozie.api;


import org.safehaus.subutai.common.protocol.EnvironmentBuildTask;
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

    UUID startServer(OozieClusterConfig config);

    UUID stopServer(OozieClusterConfig config);

    UUID checkServerStatus(OozieClusterConfig config);

    public ClusterSetupStrategy getClusterSetupStrategy(TrackerOperation po, OozieClusterConfig config,
                                                        Environment environment);

    public EnvironmentBuildTask getDefaultEnvironmentBlueprint(OozieClusterConfig config);

    UUID addNode(String clustername, String lxchostname, String nodetype);

    UUID destroyNode(final String clusterName, final String lxcHostname);
}
