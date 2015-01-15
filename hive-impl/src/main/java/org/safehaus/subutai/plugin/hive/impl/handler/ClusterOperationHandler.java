package org.safehaus.subutai.plugin.hive.impl.handler;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.safehaus.subutai.plugin.hive.impl.Commands;
import org.safehaus.subutai.plugin.hive.impl.HiveImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<HiveImpl, HiveConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private HiveConfig config;


    public ClusterOperationHandler( final HiveImpl manager, final HiveConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( HiveConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public void run()
    {
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        CommandResult result = null;
        switch ( clusterOperationType )
        {
            case START_ALL:
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    result = executeCommand( containerHost, Commands.startCommand );
                }
                break;
            case STOP_ALL:
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    result = executeCommand( containerHost, Commands.stopCommand );
                }
                break;
            case STATUS_ALL:
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    result = executeCommand( containerHost, Commands.statusCommand );
                }
                break;
        }
        NodeOperationHandler.logResults( trackerOperation, result );
    }


    private CommandResult executeCommand( ContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", command );
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void setupCluster()
    {

        ClusterSetupStrategy setupStrategy = manager.getClusterSetupStrategy( config, trackerOperation );
        try
        {
            setupStrategy.setup();

            trackerOperation.addLogDone( "Cluster setup complete" );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to setup cluster: %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {

        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        Set<ContainerHost> hiveNodes = environment.getContainerHostsByIds( config.getAllNodes() );
        for ( ContainerHost host : hiveNodes )
        {
            try
            {
                CommandResult result;
                if ( host.getId().equals( config.getServer() ) )
                {
                    result = host.execute( new RequestBuilder(
                            Commands.uninstallCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY
                                    .toLowerCase() ) );
                    host.execute( new RequestBuilder( Commands.uninstallCommand + Common.PACKAGE_PREFIX + "derby" ) );
                }
                else
                {
                    result = host.execute( new RequestBuilder(
                            Commands.uninstallCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY
                                    .toLowerCase() ) );
                }
                if ( result.hasSucceeded() )
                {
                    config.getClients().remove( host.getId() );
                    trackerOperation.addLog( HiveConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                            + " successfully." );
                }
                else
                {
                    trackerOperation.addLog(
                            "Could not uninstall " + HiveConfig.PRODUCT_KEY + " from node " + host.getHostname() );
                }
            }
            catch ( CommandException e )
            {
                trackerOperation.addLog( String.format( "Error uninstalling Hive from node %s", host.getHostname() ) );
            }
        }
        manager.getPluginDAO().deleteInfo( HiveConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Hive cluster is removed from database" );
    }
}
