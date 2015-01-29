package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigureEnvironmentClusterHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigureEnvironmentClusterHandler.class );
    private HadoopClusterConfig config;
    private TrackerOperation po;


    public ConfigureEnvironmentClusterHandler( final HadoopImpl manager, final HadoopClusterConfig config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        po = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
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
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            po.addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
            LOGGER.error( String.format( "Failed to setup cluster %s", clusterName ), e );
        }
    }
}
