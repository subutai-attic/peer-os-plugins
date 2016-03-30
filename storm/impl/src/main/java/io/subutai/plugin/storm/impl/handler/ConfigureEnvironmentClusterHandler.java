package io.subutai.plugin.storm.impl.handler;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.ClusterConfiguration;
import io.subutai.plugin.storm.impl.StormImpl;


public class ConfigureEnvironmentClusterHandler extends AbstractOperationHandler<StormImpl, StormClusterConfiguration>
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigureEnvironmentClusterHandler.class );
    private StormClusterConfiguration config;
    private TrackerOperation po;


    public ConfigureEnvironmentClusterHandler( final StormImpl manager, final StormClusterConfiguration config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        po = manager.getTracker().createTrackerOperation( StormClusterConfiguration.PRODUCT_KEY,
                String.format( "Setting up %s cluster...", config.getClusterName() ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return po.getId();
    }


    @Override
    public void run()
    {
        trackerOperation = po;
        po.addLog( "Configuring environment..." );

        try
        {
            Environment env;
            try
            {
                env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId() ), e );
                return;
            }

            try
            {
                new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }
        }
        catch ( ClusterSetupException e )
        {
            logException( String.format( "Failed to setup %s cluster %s", config.getProductKey(), clusterName ), e );
        }
    }


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        trackerOperation.addLogFailed( msg );
    }
}
