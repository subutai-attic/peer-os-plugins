package org.safehaus.subutai.plugin.mahout.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.impl.MahoutImpl;


public class NodeOperationHandler extends AbstractOperationHandler<MahoutImpl, MahoutClusterConfig>
{

    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;


    public NodeOperationHandler( final MahoutImpl manager, String clusterName, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( MahoutClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {

        MahoutClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Environment not found: %s", e ) );
            return;
        }
        ContainerHost host;
        try
        {
            host = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }


        switch ( operationType )
        {
            case INSTALL:
                installProductOnNode( host );
                break;
            case UNINSTALL:
                uninstallProductOnNode( host );
                break;
        }
    }


    private CommandResult installProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( manager.getCommands().getInstallCommand() );
            if ( result.hasSucceeded() )
            {
                config.getNodes().add( host.getId() );
                manager.getPluginDAO().saveInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLogDone(
                        MahoutClusterConfig.PRODUCT_KEY + " is installed on node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not install " + MahoutClusterConfig.PRODUCT_KEY + " to node " + hostname );
            }
        }
        catch ( CommandException e )
        {
            trackerOperation
                    .addLogFailed( "Could not install " + MahoutClusterConfig.PRODUCT_KEY + " to node " + hostname );
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( manager.getCommands().getUninstallCommand() );
            if ( result.hasSucceeded() )
            {
                config.getNodes().remove( host.getId() );
                manager.getPluginDAO().saveInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLogDone(
                        MahoutClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not uninstall " + MahoutClusterConfig.PRODUCT_KEY + " from node " + hostname );
            }
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed(
                    "Could not uninstall " + MahoutClusterConfig.PRODUCT_KEY + " from node " + hostname );
        }
        return result;
    }
}
