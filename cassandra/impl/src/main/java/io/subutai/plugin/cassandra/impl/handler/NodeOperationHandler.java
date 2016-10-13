package io.subutai.plugin.cassandra.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.ClusterConfiguration;
import io.subutai.plugin.cassandra.impl.Commands;


/**
 * This class handles operations that are related to just one node.
 */
public class NodeOperationHandler extends AbstractOperationHandler<CassandraImpl, CassandraClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );

    private String clusterName;
    private String id;
    private NodeOperationType operationType;


    public NodeOperationHandler( final CassandraImpl manager, final String clusterName, final String id,
                                 NodeOperationType operationType )
    {
        super( manager, clusterName );
        this.id = id;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( CassandraClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        CassandraClusterConfig config = manager.getCluster( clusterName );
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
            trackerOperation.addLogFailed( "Cluster environment not found" );
            return;
        }

        EnvironmentContainerHost host = null;
        try
        {
            host = environment.getContainerHostByHostname( id );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", id ) );
            return;
        }

        if ( !config.getSeedNodes().contains( host.getHostname() ) )
        {
            trackerOperation.addLogFailed( String.format( "Node %s does not belong to %s cluster.", id, clusterName ) );
            return;
        }

        try
        {
            CommandResult result;
            switch ( operationType )
            {
                case START:
                    result = host.execute( new RequestBuilder( Commands.START_COMMAND ) );
                    logResults( trackerOperation, result );
                    break;
                case STOP:
                    result = host.execute( new RequestBuilder( Commands.STOP_COMMAND ) );
                    logResults( trackerOperation, result );
                    break;
                case STATUS:
                    result = host.execute( new RequestBuilder( Commands.STATUS_COMMAND ) );
                    logResults( trackerOperation, result );
                    break;
                case DESTROY:
                    destroyNode( host );
                    break;
            }
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    public void destroyNode( EnvironmentContainerHost host )
    {

        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        try
        {
            try
            {
                Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                CassandraClusterConfig config = manager.getCluster( clusterName );
                config.getSeedNodes().remove( host.getHostname() );

                // configure cluster again
                ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
                try
                {
                    configurator.removeNode( host );
                    configurator.configureCluster( config, environment );
                }
                catch ( ClusterConfigurationException e )
                {
                    trackerOperation.addLogFailed(
                            String.format( "Error during reconfiguration after removing node %s from cluster: %s",
                                    host.getHostname(), config.getClusterName() ) );
                    LOG.error( String.format( "Error during reconfiguration after removing node %s from cluster: %s",
                            host.getHostname(), config.getClusterName() ), e );
                    e.printStackTrace();
                }

                // saving config
                manager.saveConfig( config );

                trackerOperation.addLog( "Cluster information is updated" );
                trackerOperation
                        .addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
                LOG.info( String.format( "Container %s is removed from cluster", host.getHostname() ) );
            }
            catch ( EnvironmentNotFoundException e )
            {
                trackerOperation
                        .addLogFailed( String.format( "Environment not found: %s", config.getEnvironmentId() ) );
                LOG.error( String.format( "Environment not found: %s", config.getEnvironmentId() ), e );
                e.printStackTrace();
            }
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Error in saving configuration of cluster: %s", config.getClusterName() ) );
            LOG.error( String.format( "Error in saving configuration of cluster: %s", config.getClusterName() ), e );
            e.printStackTrace();
        }
    }


    public static void logResults( TrackerOperation po, CommandResult result )
    {
        Preconditions.checkNotNull( result );

        if ( result.hasSucceeded() || !Strings.isNullOrEmpty( result.getStdOut() ) )
        {
            po.addLogDone( result.getStdOut() );
        }
        else
        {
            po.addLogFailed( result.getStdErr() );
        }
    }
}
