package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles uninstall mongo cluster operation
 */
public class UninstallOperationHandler extends AbstractMongoOperationHandler<MongoImpl, MongoClusterConfig>
{
    private final TrackerOperation po;
    private CommandUtil commandUtil = new CommandUtil();
    private static final Logger LOGGER = LoggerFactory.getLogger( UninstallOperationHandler.class );

    public UninstallOperationHandler( MongoImpl manager, String clusterName )
    {
        super( manager, manager.getCluster( clusterName ) );
        po = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Destroying cluster %s", clusterName ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return po.getId();
    }


    @Override
    public void run()
    {
        MongoClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            po.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        try
        {
            po.addLog( "Removing subscription from environment." );
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            manager.unsubscribeFromAlerts( environment );


            po.addLog( "Stopping mongo containers." );
            Set<ContainerHost> containerHosts = environment.getContainerHostsByIds( config.getAllNodeIds() );
            List<CommandResult> commandResults = new ArrayList<>();
            for ( final ContainerHost containerHost : containerHosts )
            {
                commandResults.add( commandUtil.execute(
                        new RequestBuilder( Commands.getStopMongodbService().getCommand() ),
                        containerHost ) );
            }
            logResults( po, commandResults );

        }
        catch ( ContainerHostNotFoundException | EnvironmentNotFoundException |
                MonitorException | CommandException ex )
        {
            po.addLogFailed( "Operations failed" );
            LOGGER.error( "Operations failed", ex );
            return;
        }

        po.addLog( "Deleting cluster information from database.." );

        manager.getPluginDAO().deleteInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName() );
        po.addLogDone( "Cluster destroyed." );
    }
}
