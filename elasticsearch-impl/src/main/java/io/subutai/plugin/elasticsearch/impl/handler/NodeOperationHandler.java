package io.subutai.plugin.elasticsearch.impl.handler;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


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
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
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
                host = environment.getContainerHostByHostname( hostname );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }

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
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
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
