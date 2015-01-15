package org.safehaus.subutai.plugin.nutch.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.nutch.api.NutchConfig;
import org.safehaus.subutai.plugin.nutch.impl.Commands;
import org.safehaus.subutai.plugin.nutch.impl.NutchImpl;


public class NodeOperationHandler extends AbstractOperationHandler<NutchImpl, NutchConfig>
{
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NutchConfig config;
    Commands commands = new Commands();
    CommandUtil commandUtil = new CommandUtil();


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
            CommandResult result = commandUtil.execute( commands.getCheckInstallationCommand(), node );
            if ( result.getStdOut().contains( NutchConfig.PRODUCT_PACKAGE ) )
            {
                trackerOperation
                        .addLogFailed( String.format( "Node %s already has Nutch installed", node.getHostname() ) );
                return;
            }
            else if ( !result.getStdOut()
                             .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
            {
                trackerOperation
                        .addLogFailed( String.format( "Node %s has no Hadoop installation", node.getHostname() ) );
                return;
            }

            commandUtil.execute( commands.getInstallCommand(), node );

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
            commandUtil.execute( commands.getUninstallCommand(), node );

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
