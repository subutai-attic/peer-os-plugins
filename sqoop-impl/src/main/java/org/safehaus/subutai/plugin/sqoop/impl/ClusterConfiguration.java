package org.safehaus.subutai.plugin.sqoop.impl;


import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface<SqoopConfig>
{

    @Override
    public void configureCluster( SqoopConfig config, Environment environment ) throws ClusterConfigurationException
    {
        // no configurations for Sqoop installation
        // this is a no-op method!!!
    }
}

