package io.subutai.plugin.mongodb.impl.handler;


import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.NodeType;
import io.subutai.plugin.mongodb.impl.ClusterConfiguration;
import io.subutai.plugin.mongodb.impl.MongoImpl;
import io.subutai.plugin.mongodb.impl.common.CommandDef;
import io.subutai.plugin.mongodb.impl.common.Commands;


/**
 * This class handles operations that are related to just one node.
 */
public class NodeOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
{

    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NodeType nodeType;


    public NodeOperationHandler( final MongoImpl manager, final String clusterName, final String hostname,
                                 NodeType nodeType, NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.nodeType = nodeType;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


	public NodeOperationHandler( final MongoImpl manager, final String clusterName, final String hostname,
								 NodeOperationType operationType )
	{
		super( manager, clusterName );
		this.hostname = hostname;
		this.clusterName = clusterName;
		this.operationType = operationType;
		this.trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
				String.format( "Creating %s tracker object...", clusterName ) );
	}

    @Override
    public void run()
    {
        MongoClusterConfig config = manager.getCluster( clusterName );
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
            host = environment.getContainerHostByHostname ( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }

        if ( !config.getAllNodes().contains( host.getId() ) )
        {
            trackerOperation
                    .addLogFailed( String.format( "Node %s does not belong to %s cluster.", hostname, clusterName ) );
            return;
        }

        switch ( operationType )
        {
            case START:
                startNode( host );
                break;
            case STOP:
                stopNode( host );
                break;
            case STATUS:
                checkNode( host );
                break;
            case DESTROY:
                destroyNode( host );
                break;
        }
    }


    private boolean checkNode( EnvironmentContainerHost host )
    {
        CommandDef commandDef = null;
        switch ( nodeType )
        {
            case CONFIG_NODE:
                commandDef = Commands.getCheckConfigServer();
                break;
            case ROUTER_NODE:
                commandDef = Commands.getCheckRouterNode();
                break;
            case DATA_NODE:
                commandDef = Commands.getCheckDataNode();
                break;
        }
        try
        {
            CommandResult commandResult = host.execute( commandDef.build( true ).withTimeout( 20 ) );
            if ( !commandResult.getStdOut().isEmpty() )
            {
                trackerOperation.addLogDone( nodeType.name() + " service is running on node " + host.getHostname() );
                return true;
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        trackerOperation.addLogDone( nodeType.name() + " service is NOT running on node " + host.getHostname() );
        return false;
    }


    private void startNode( EnvironmentContainerHost host )
    {
        switch ( nodeType )
        {
            case CONFIG_NODE:
                executeCommand( Commands.getStartConfigServerCommand( config.getCfgSrvPort() ).build( true ), host );
                break;
            case ROUTER_NODE:
                Set<EnvironmentContainerHost> configServers = new HashSet<>();
                for ( String id : config.getConfigHosts() )
                {
                    configServers.add( findHost( id ) );
                }
                executeCommand( Commands.getStartRouterCommandLine( config.getRouterPort(), config.getCfgSrvPort(),
                        config.getDomainName(), configServers ).build( true ), host );
                break;
            case DATA_NODE:
                executeCommand( Commands.getStartDataNodeCommandLine( config.getDataNodePort() ).build( true ), host );
                break;
        }
        trackerOperation.addLogDone( "Mongo service on " + host.getHostname() + " is started." );
    }


    private void stopNode( EnvironmentContainerHost host )
    {
        executeCommand( Commands.getStopNodeCommand().build(), host );
        trackerOperation.addLogDone( "Mongo service on " + host.getHostname() + " is stopped." );
    }


    private void destroyNode( EnvironmentContainerHost host )
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        try
        {
            MongoClusterConfig config = manager.getCluster( clusterName );
            if ( nodeType.equals( NodeType.ROUTER_NODE ) )
            {
                config.getRouterHosts().remove( host.getId() );
            }
            else if ( nodeType.equals( NodeType.DATA_NODE ) )
            {
                config.getDataHosts().remove( host.getId() );
            }
            manager.saveConfig( config );

            // configure cluster again
            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
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


    private void executeCommand( RequestBuilder command, EnvironmentContainerHost host )
    {
        try
        {
            host.execute( command );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private EnvironmentContainerHost findHost( String hostId )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            try
            {
                return environment.getContainerHostById( hostId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }


    public static void logResults( TrackerOperation po, CommandResult result )
    {
        Preconditions.checkNotNull( result );
        StringBuilder log = new StringBuilder();
        log.append( "UNKNOWN" );
        if ( result.getExitCode() == 0 )
        {
            log.append( result.getStdOut() );
        }

        if ( result.getExitCode() == 768 )
        {
            log.append( "cassandra is not running" );
        }
        po.addLogDone( log.toString() );
    }
}
