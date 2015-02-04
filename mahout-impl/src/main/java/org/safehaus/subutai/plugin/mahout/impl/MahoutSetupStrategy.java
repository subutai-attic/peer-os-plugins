package org.safehaus.subutai.plugin.mahout.impl;


import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;


abstract class MahoutSetupStrategy implements ClusterSetupStrategy
{

    final MahoutImpl manager;
    final MahoutClusterConfig config;
    final TrackerOperation trackerOperation;


    public MahoutSetupStrategy( MahoutImpl manager, MahoutClusterConfig config, TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.trackerOperation = po;
    }
}
