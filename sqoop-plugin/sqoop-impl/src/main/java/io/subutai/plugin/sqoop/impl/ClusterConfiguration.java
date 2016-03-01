package io.subutai.plugin.sqoop.impl;


import io.subutai.common.environment.Environment;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
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

