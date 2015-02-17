package org.safehaus.subutai.plugin.storm.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
            Environment env = null;
            try
            {
                env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException(
                        String.format( "Couldn't find environment by id: %s", config.getEnvironmentId().toString() ),
                        e );
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
