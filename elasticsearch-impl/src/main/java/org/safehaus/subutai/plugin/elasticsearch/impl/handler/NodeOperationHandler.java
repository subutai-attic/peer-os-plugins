package org.safehaus.subutai.plugin.elasticsearch.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


/**
 * This class handles operations that are related to just one node.
 */
public class NodeOperationHandler extends AbstractOperationHandler<ElasticsearchImpl, ElasticsearchClusterConfiguration>
{

    private String hostname;
    private NodeOperationType operationType;
    CommandUtil commandUtil = new CommandUtil();


    public NodeOperationHandler( final ElasticsearchImpl manager, final String clusterName, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker()
                                       .createTrackerOperation( ElasticsearchClusterConfiguration.PRODUCT_KEY,
                                               String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        ContainerHost host = environment.getContainerHostByHostname( hostname );

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "Container %s not found in environment", hostname ) );
            return;
        }

        try
        {
            CommandResult result = null;
            switch ( operationType )
            {
                case START:
                    result = commandUtil.execute( manager.getCommands().getStartCommand(), host );

                    break;
                case STOP:
                    result = commandUtil.execute( manager.getCommands().getStopCommand(), host );
                    break;
                case STATUS:
                    result = commandUtil.execute( manager.getCommands().getStatusCommand(), host );
                    break;
                case INSTALL:
                    addNode( host );
                    break;
                case UNINSTALL:
                    removeNode( host );
                    break;
            }
            logResults( trackerOperation, result );
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Operation failed, %s", e.getMessage() ) );
        }
    }


    private void removeNode( final ContainerHost host ) throws ClusterException, CommandException
    {
        //check if node belongs to this cluster
        if ( !config.getNodes().contains( host.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to ES cluster %s", host.getHostname(),
                    config.getClusterName() ) );
        }

        //check if node is connected
        if ( !host.isConnected() )
        {
            throw new ClusterException( String.format( "Node %s is disconnected", host.getHostname() ) );
        }

        //check if ES is installed
        CommandResult result = commandUtil.execute( manager.getCommands().getCheckInstallationCommand(), host );
        if ( result.getStdOut().contains( ElasticsearchClusterConfiguration.PACKAGE_NAME ) )
        {
            //uninstall ES from the node
            trackerOperation.addLog( String.format( "Uninstalling ES from %s...", host.getHostname() ) );
            commandUtil.execute( manager.getCommands().getUninstallCommand(), host );
        }

        config.getNodes().remove( host.getId() );

        manager.saveConfig( config );

        trackerOperation.addLogDone( "Node removed" );
    }


    private void addNode( ContainerHost host ) throws ClusterException, CommandException
    {
        //check if node already belongs to this cluster
        if ( config.getNodes().contains( host.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s already belongs to ES cluster %s", host.getHostname(),
                    config.getClusterName() ) );
        }

        //check if node is connected
        if ( !host.isConnected() )
        {
            throw new ClusterException( String.format( "Node %s is disconnected", host.getHostname() ) );
        }

        //check if ES is installed
        CommandResult result = commandUtil.execute( manager.getCommands().getCheckInstallationCommand(), host );
        if ( !result.getStdOut().contains( ElasticsearchClusterConfiguration.PACKAGE_NAME ) )
        {
            //install ES on the node
            trackerOperation.addLog( String.format( "Installing ES on %s...", host.getHostname() ) );
            commandUtil.execute( manager.getCommands().getInstallCommand(), host );
        }

        //configure node
        commandUtil.execute( manager.getCommands().getConfigureCommand( config.getClusterName() ), host );

        config.getNodes().add( host.getId() );

        manager.saveConfig( config );

        trackerOperation.addLogDone( "Node added" );

        try
        {
            manager.subscribeToAlerts( host );
        }
        catch ( MonitorException e )
        {
            throw new ClusterException( e );
        }
    }


    public static void logResults( TrackerOperation po, CommandResult result )
    {
        if ( result != null )
        {
            StringBuilder log = new StringBuilder();
            String status;
            if ( result.getExitCode() == 0 )
            {
                status = result.getStdOut();
            }
            else if ( result.getExitCode() == 768 )
            {
                status = "elasticsearch is not running";
            }
            else
            {
                status = result.getStdOut();
            }
            log.append( String.format( "%s", status ) );
            po.addLogDone( log.toString() );
        }
    }
}
