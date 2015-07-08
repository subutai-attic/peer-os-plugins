package io.subutai.plugin.mahout.impl;


import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;


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
