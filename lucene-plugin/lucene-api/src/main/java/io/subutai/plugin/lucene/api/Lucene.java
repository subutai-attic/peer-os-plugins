package io.subutai.plugin.lucene.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Lucene extends ApiBase<LuceneConfig>
{
    public UUID addNode( String clusterName, String lxcHostname );

    public UUID uninstallNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, LuceneConfig config, TrackerOperation po );

    public void saveConfig( final LuceneConfig config ) throws ClusterException;

    public void deleteConfig( final LuceneConfig config ) throws ClusterException;
}
