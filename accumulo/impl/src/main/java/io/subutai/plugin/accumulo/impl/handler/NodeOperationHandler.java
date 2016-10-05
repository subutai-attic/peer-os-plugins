package io.subutai.plugin.accumulo.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.OperationType;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.ClusterConfiguration;
import io.subutai.plugin.accumulo.impl.Commands;


public class NodeOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );

    private String hostId;
    private OperationType operationType;
    private NodeType nodeType;
    private Environment environment;
    private EnvironmentContainerHost node;


    public NodeOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config, final String hostId,
                                 final OperationType operationType, final NodeType nodeType )
    {
        super( manager, config );
        this.hostId = hostId;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on node %s", operationType.name(), hostId ) );
    }


    @Override
    public void run()
    {
        try
        {
            if ( manager.getCluster( clusterName ) == null )
            {
                throw new ClusterException( String.format( "Cluster with name %s does not exist", clusterName ) );
            }

            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
            }

            try
            {
                node = environment.getContainerHostById( hostId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Node not found in environment by id %s", node.getHostname() ) );
            }


            if ( !node.isConnected() )
            {
                throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
            }


            switch ( operationType )
            {
                case START:
                    startNode();
                    break;
                case STOP:
                    stopNode();
                    break;
                case STATUS:
                    checkNode();
                    break;
                case INSTALL:
                    addNode();
                    break;
                case UNINSTALL:
                    removeNode();
                    break;
            }

            trackerOperation.addLogDone( "" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in NodeOperationHandler", e );
            trackerOperation
                    .addLogFailed( String.format( "Operation %s failed: %s", operationType.name(), e.getMessage() ) );
        }
    }


    private void removeNode() throws ClusterException
    {
        ClusterConfiguration configuration = new ClusterConfiguration( manager, trackerOperation );
        try
        {
            configuration.deleteNode( node );
            config.getSlaves().remove( node.getId() );

            configuration.reconfigureNodes( config, environment, node.getHostname() );

            manager.saveConfig( config );
            trackerOperation.addLogDone( "Node removed successfully" );
        }
        catch ( CommandException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Error on container %s: %s", node.getHostname(), e.getMessage() ) );
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "Can not find container in environment" );
            e.printStackTrace();
        }
    }


    private void addNode() throws ClusterException
    {
        try
        {
            node.execute( Commands.getAptUpdate() );
            node.execute( Commands.getInstallCommand() );

            ClusterConfiguration configuration = new ClusterConfiguration( manager, trackerOperation );
            config.getSlaves().add( node.getId() );
            configuration.addNode( config, environment, node );

            manager.saveConfig( config );
            trackerOperation.addLogDone( "Node added successfully" );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Error on container: %s", e.getMessage() ) );
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "Can not find container in environment" );
            e.printStackTrace();
        }
    }


    private void checkNode() throws ClusterException
    {
        CommandResult result = executeCommand( node, Commands.getStatusCommand() );
        if ( !result.getStdOut().contains( "QuorumPeerMain" ) )
        {
            try
            {
                node.execute( Commands.getStartZkServerCommand() );
            }
            catch ( CommandException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Error on container %s: %s", node.getHostname(), e.getMessage() ) );
                e.printStackTrace();
            }
        }
    }


    private void stopNode() throws ClusterException
    {
        try
        {
            switch ( nodeType )
            {
                case MASTER_NODE:
                    Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );

                    node.execute( Commands.getStopMasterCommand() );

                    for ( final EnvironmentContainerHost slave : slaves )
                    {
                        slave.execute( Commands.getStopSlaveCommand() );
                    }
                    break;
                case SLAVE_NODE:
                    node.execute( Commands.getStopSlaveCommand() );
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "Can not find container in environment" );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( "Error on container" );
            e.printStackTrace();
        }
    }


    private void startNode() throws ClusterException
    {
        try
        {
            switch ( nodeType )
            {
                case MASTER_NODE:
                    Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );

                    node.execute( Commands.getStartMasterCommand() );

                    for ( final EnvironmentContainerHost slave : slaves )
                    {
                        slave.execute( Commands.getStartSlaveCommand() );
                    }
                    break;
                case SLAVE_NODE:
                    node.execute( Commands.getStartSlaveCommand() );
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "Can not find container in environment" );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( "Error on container" );
            e.printStackTrace();
        }
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command ) throws ClusterException
    {
        CommandResult result = null;
        try
        {
            result = host.execute( command );
            trackerOperation.addLog( result.getStdOut() );
        }
        catch ( CommandException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Error on container %s: %s", host.getHostname(), e.getMessage() ) );
            e.printStackTrace();
        }
        return result;
    }
}
