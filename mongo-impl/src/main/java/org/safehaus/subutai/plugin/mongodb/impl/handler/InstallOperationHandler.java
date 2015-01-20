package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;


/**
 * Handles install mongo cluster operation
 */
public class InstallOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
{

    private final TrackerOperation po;
    private final MongoClusterConfig config;


    public InstallOperationHandler( final MongoImpl manager, final MongoClusterConfig config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        po = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
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

        po.addLog( "Building environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager()
                                     .buildEnvironment( manager.getDefaultEnvironmentBlueprint( config ) );
            config.setEnvironmentId( env.getId() );
            for ( final ContainerHost containerHost : env.getContainerHosts() )
            {
                if ( config.getConfigHostIds().size() != config.getNumberOfConfigServers() )
                {
                    config.getConfigHostIds().add( containerHost.getId() );
                }
                else if ( config.getRouterHostIds().size() != config.getNumberOfRouters() )
                {
                    config.getRouterHostIds().add( containerHost.getId() );
                }
                else if ( config.getDataHostIds().size() != config.getNumberOfDataNodes() )
                {
                    config.getDataHostIds().add( containerHost.getId() );
                }
            }
            ClusterSetupStrategy clusterSetupStrategy = manager.getClusterSetupStrategy( env, config, po );
            clusterSetupStrategy.setup();

            po.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
            manager.subscribeToAlerts( env );
        }
        catch ( EnvironmentBuildException | ClusterSetupException | MonitorException e )
        {
            po.addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }
}
