package org.safehaus.subutai.plugin.hive.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.safehaus.subutai.plugin.hive.impl.Commands;
import org.safehaus.subutai.plugin.hive.impl.HiveImpl;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to just one node.
 */
public class NodeOperationHandler extends AbstractOperationHandler<HiveImpl, HiveConfig>
{

    private String hostname;
    private NodeOperationType operationType;


    public NodeOperationHandler( final HiveImpl manager, final String clusterName, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( HiveConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {

        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        ContainerHost host = environment.getContainerHostByHostname( hostname );
        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }

        try
        {
            CommandResult result = null;
            switch ( operationType )
            {
                case START:
                    result = host.execute( new RequestBuilder( Commands.startCommand ) );
                    break;
                case STOP:
                    result = host.execute( new RequestBuilder( Commands.stopCommand ) );
                    break;
                case STATUS:
                    result = host.execute( new RequestBuilder( Commands.statusCommand ) );
                    break;
                case RESTART:
                    result = host.execute( new RequestBuilder( Commands.restartCommand ) );
                    break;
                case UNINSTALL:
                    result = uninstallProductOnNode( host );
                    break;
                case INSTALL:
                    result = installProductOnNode( host );
                    break;
            }
            logResults( trackerOperation, result );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    public static void logResults( TrackerOperation po, CommandResult result )
    {
        if ( result != null )
        {
            Preconditions.checkNotNull( result );
            String status = "UNKNOWN";
            if ( result.getExitCode() == 0 )
            {
                status = result.getStdOut();
            }
            else if ( result.getExitCode() == 768 )
            {
                status = "Hive Thrift Server is not running";
            }

            po.addLogDone( status );
        }
    }


    private CommandResult installProductOnNode( ContainerHost host )
    {

        CommandResult result = null;
        if ( config.getAllNodes().contains( host.getId() ) )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster %s already contains node %s", clusterName, host.getHostname() ) );
        }
        else
        {
            try
            {
                result = host.execute( new RequestBuilder(
                        Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ) );
                if ( result.hasSucceeded() )
                {
                    config.getClients().add( host.getId() );
                    manager.getPluginDAO().saveInfo( HiveConfig.PRODUCT_KEY, config.getClusterName(), config );
                    trackerOperation.addLog(
                            HiveConfig.PRODUCT_KEY + " is installed on node " + host.getHostname() + " successfully." );
                }
                else
                {
                    result = null;
                    trackerOperation
                            .addLogFailed( "Could not install " + HiveConfig.PRODUCT_KEY + " to node " + hostname );
                }
            }
            catch ( CommandException e )
            {
                trackerOperation.addLogFailed( String.format( "Adding node failed: %s", e.getMessage() ) );
            }
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        CommandResult result = null;

        if ( !config.getAllNodes().contains( host.getId() ) )
        {
            trackerOperation.addLogFailed(
                    String.format( "Node %s does not belong to cluster %s", host.getHostname(), clusterName ) );
        }
        else
        {
            try
            {
                result = host.execute( new RequestBuilder(
                        Commands.uninstallCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ) );
                if ( result.hasSucceeded() )
                {
                    config.getClients().remove( host.getId() );
                    manager.getPluginDAO().saveInfo( HiveConfig.PRODUCT_KEY, config.getClusterName(), config );
                    trackerOperation.addLog( HiveConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                            + " successfully." );
                }
                else
                {
                    result = null;
                    trackerOperation
                            .addLogFailed( "Could not uninstall " + HiveConfig.PRODUCT_KEY + " from node " + hostname );
                }
            }
            catch ( CommandException e )
            {
                trackerOperation.addLogFailed( String.format( "Removing node failed: %s", e.getMessage() ) );
            }
        }
        return result;
    }
}
