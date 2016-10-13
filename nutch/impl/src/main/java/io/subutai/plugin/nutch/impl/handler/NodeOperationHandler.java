package io.subutai.plugin.nutch.impl.handler;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.nutch.api.NutchConfig;
import io.subutai.plugin.nutch.impl.Commands;
import io.subutai.plugin.nutch.impl.NutchImpl;


public class NodeOperationHandler extends AbstractOperationHandler<NutchImpl, NutchConfig>
{
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NutchConfig config;


    public NodeOperationHandler( final NutchImpl manager, String clusterName, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( NutchConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
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


    private void installProductOnNode( ContainerHost node )
    {
        if ( config.getNodes().contains( node.getId() ) )
        {
            trackerOperation.addLogFailed( String.format( "Node %s already belongs to cluster %s", node.getHostname(),
                    config.getClusterName() ) );
            return;
        }

        try
        {
            CommandResult result = node.execute( Commands.getCheckInstallationCommand() );
            if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
            {
                trackerOperation
                        .addLogFailed( String.format( "Node %s already has Nutch installed", node.getHostname() ) );
                return;
            }

            node.execute( Commands.getInstallCommand() );

            config.getNodes().add( node.getId() );

            manager.saveConfig( config );

            trackerOperation.addLogDone(
                    NutchConfig.PRODUCT_KEY + " is installed on node " + node.getHostname() + " successfully." );
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to add node: %s", e.getMessage() ) );
        }
    }


    private void uninstallProductOnNode( ContainerHost node )
    {
        try
        {
            node.execute( Commands.getUninstallCommand() );

            config.getNodes().remove( node.getId() );

            manager.saveConfig( config );

            trackerOperation.addLogDone(
                    NutchConfig.PRODUCT_KEY + " is uninstalled from node " + node.getHostname() + " successfully." );
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to uninstall Nutch: %s", e.getMessage() ) );
        }
    }
}
