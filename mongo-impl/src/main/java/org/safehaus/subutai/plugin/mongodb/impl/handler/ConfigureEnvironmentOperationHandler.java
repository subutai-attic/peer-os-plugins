package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ConfigureEnvironmentOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
{
    private final MongoClusterConfig config;
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigureEnvironmentOperationHandler.class );


    public ConfigureEnvironmentOperationHandler( final MongoImpl manager, final MongoClusterConfig config )
    {
        super( manager, config );
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Configuring %s cluster...", config.getClusterName() ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return trackerOperation.getId();
    }


    @Override
    public void run()
    {

        trackerOperation.addLog( "Configuring environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s configured successfully", clusterName ) );
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            LOGGER.error( String.format( "Failed to configure cluster %s", clusterName ), e );
            trackerOperation.addLogFailed( String.format( "Failed to configure cluster %s", clusterName ) );
        }
    }
}
