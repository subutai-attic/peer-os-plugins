package io.subutai.plugin.elasticsearch.impl.handler;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.Commands;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


/**
 * This class handles operations that are related to just one node.
 */
public class NodeOperationHandler extends AbstractOperationHandler<ElasticsearchImpl, ElasticsearchClusterConfiguration>
{

    private String hostId;
    private NodeOperationType operationType;
    CommandUtil commandUtil = new CommandUtil();


    public NodeOperationHandler( final ElasticsearchImpl manager, final String clusterName, final String hostId,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostId = hostId;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker()
                                       .createTrackerOperation( ElasticsearchClusterConfiguration.PRODUCT_KEY,
                                               String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        ContainerHost host = null;
        if ( environment != null )
        {
            try
            {
                host = environment.getContainerHostById( hostId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "Container %s not found in environment", hostId ) );
            return;
        }

        if ( !host.isConnected() )
        {
            trackerOperation.addLogFailed( String.format( "Container %s is not connected", hostId ) );
            return;
        }

        try
        {
            CommandResult result = null;
            switch ( operationType )
            {
                case START:
                    result = host.execute( Commands.getStartCommand() );
                    break;
                case STOP:
                    result = host.execute( Commands.getStopCommand() );
                    break;
                case STATUS:
                    result = host.execute( Commands.getStatusCommand() );
                    break;
                case UNINSTALL:
                    removeNode( host );
                    break;
            }
            logResults( trackerOperation, result );
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation.addLogFailed( "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" );
            trackerOperation.addLogFailed( String.format( "Operation failed, %s", e.getMessage() ) );
            trackerOperation.addLogFailed( "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" );
        }
    }


    private void removeNode( final ContainerHost host ) throws ClusterException, CommandException
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        try
        {
            ElasticsearchClusterConfiguration config = manager.getCluster( clusterName );
            config.getNodes().remove( host.getId() );
            manager.saveConfig( config );
            // configure cluster again
            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            try
            {
                configurator
                        .configureCluster( config, environmentManager.loadEnvironment( config.getEnvironmentId() ) );
            }
            catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
            {
                e.printStackTrace();
            }
            trackerOperation.addLog( String.format( "Cluster information is updated" ) );
            trackerOperation.addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
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
