package io.subutai.plugin.lucene.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.webui.api.WebuiModule;


public interface Lucene extends ApiBase<LuceneConfig>
{
    public UUID addNode( String clusterName, String lxcHostname );

    public UUID uninstallNode( String clusterName, String lxcHostname );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, LuceneConfig config, TrackerOperation po );

    public void saveConfig( final LuceneConfig config ) throws ClusterException;

    public void deleteConfig( final LuceneConfig config ) throws ClusterException;

    public WebuiModule getWebModule();

    public void setWebModule( final WebuiModule webModule );
}
