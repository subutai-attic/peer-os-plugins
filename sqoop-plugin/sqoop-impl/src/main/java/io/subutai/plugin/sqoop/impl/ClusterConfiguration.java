package io.subutai.plugin.sqoop.impl;


import io.subutai.common.environment.Environment;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.sqoop.api.SqoopConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface<SqoopConfig>
{

    @Override
    public void configureCluster( SqoopConfig config, Environment environment ) throws ClusterConfigurationException
    {
        // no configurations for Sqoop installation
        // this is a no-op method!!!
    }
}

