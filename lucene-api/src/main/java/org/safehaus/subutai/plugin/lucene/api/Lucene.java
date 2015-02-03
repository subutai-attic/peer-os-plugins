package org.safehaus.subutai.plugin.lucene.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Lucene extends ApiBase<LuceneConfig>
{
    public UUID addNode( String clusterName, String lxcHostname );

    public UUID uninstallNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, LuceneConfig config, TrackerOperation po );
}
