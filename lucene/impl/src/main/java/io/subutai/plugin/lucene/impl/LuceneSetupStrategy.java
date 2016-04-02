package io.subutai.plugin.lucene.impl;


import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.lucene.api.LuceneConfig;


abstract class LuceneSetupStrategy implements ClusterSetupStrategy
{

    final LuceneImpl manager;
    final LuceneConfig config;
    final TrackerOperation trackerOperation;


    public LuceneSetupStrategy( LuceneImpl manager, LuceneConfig config, TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.trackerOperation = po;
    }
}
