package io.subutai.plugin.sqoop.impl;


import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.sqoop.api.SqoopConfig;


abstract class SqoopSetupStrategy implements ClusterSetupStrategy
{

    final SqoopImpl manager;
    final SqoopConfig config;
    final Environment environment;
    final TrackerOperation to;


    public SqoopSetupStrategy( SqoopImpl manager, SqoopConfig config, Environment environment, TrackerOperation to )
    {
        this.manager = manager;
        this.config = config;
        this.environment = environment;
        this.to = to;
    }


    public void checkConfig() throws ClusterSetupException
    {

        String m = "Invalid configuration: ";

        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            throw new ClusterSetupException( m + "name is not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    m + String.format( "Sqoop installation already exists: %s", config.getClusterName() ) );
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Environment not specified" );
        }

        if ( config.getNodes() == null || config.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( m + "Target nodes not specified" );
        }
    }


    void configure() throws ClusterSetupException
    {
        ClusterConfiguration cc = new ClusterConfiguration();
        try
        {
            cc.configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException ex )
        {
            throw new ClusterSetupException( ex );
        }
    }
}

