package org.safehaus.subutai.plugin.lucene.impl;


import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;


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
