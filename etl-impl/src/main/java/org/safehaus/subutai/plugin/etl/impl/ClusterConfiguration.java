package org.safehaus.subutai.plugin.etl.impl;


import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface<ETLConfig>
{

    @Override
    public void configureCluster( ETLConfig config, Environment environment ) throws ClusterConfigurationException
    {
        // no configurations for Sqoop installation
        // this is a no-op method!!!
    }

}

