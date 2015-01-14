package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;


/**
 * Created by talas on 1/8/15.
 */
public class ConfigureEnvironmentOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
{
    private final TrackerOperation po;
    private final MongoClusterConfig config;


    public ConfigureEnvironmentOperationHandler( final MongoImpl manager, final MongoClusterConfig config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        po = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Configuring %s cluster...", config.getClusterName() ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return po.getId();
    }


    @Override
    public void run()
    {

        po.addLog( "Configuring environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy = manager.getClusterSetupStrategy( env, config, po );
            clusterSetupStrategy.setup();

            po.addLogDone( String.format( "Cluster %s configured successfully", clusterName ) );
        }
        catch ( ClusterSetupException e )
        {
            po.addLogFailed( String.format( "Failed to configure cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }
}
