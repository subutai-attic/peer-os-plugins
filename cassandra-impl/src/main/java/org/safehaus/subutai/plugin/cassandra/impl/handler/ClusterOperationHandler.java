package org.safehaus.subutai.plugin.cassandra.impl.handler;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.cassandra.api.CassandraClusterConfig;
import org.safehaus.subutai.plugin.cassandra.impl.CassandraImpl;
import org.safehaus.subutai.plugin.cassandra.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.cassandra.impl.Commands;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<CassandraImpl, CassandraClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private CassandraClusterConfig config;


    public ClusterOperationHandler( final CassandraImpl manager, final CassandraClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( CassandraClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
                startNStopCluster( config, ClusterOperationType.START_ALL );
                break;
            case STOP_ALL:
                startNStopCluster( config, ClusterOperationType.STOP_ALL );
                break;
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
            case ADD:
                addNode();
                break;
            case REMOVE:
                removeCluster();
                break;
        }
    }


    private void startNStopCluster( CassandraClusterConfig config, ClusterOperationType type )
    {
        for ( UUID uuid : config.getNodes() )
        {
            try
            {
                ContainerHost host = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                            .getContainerHostById( uuid );
                switch ( type )
                {
                    case START_ALL:
                        host.execute( new RequestBuilder( Commands.startCommand ) );
                        break;
                    case STOP_ALL:
                        host.execute( new RequestBuilder( Commands.stopCommand ) );
                        break;
                }
            }
            catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException e )
            {
                trackerOperation.addLogFailed( "Failed to %s " +
                        ( type == ClusterOperationType.START_ALL ? "start" : "stop" )  + config.getClusterName() + " cluster" );
                e.printStackTrace();
            }
        }
        trackerOperation.addLogDone( String.format( "%s cluster %s successfully", config.getClusterName(),
                type == ClusterOperationType.START_ALL ? "started" : "stopped" ) );
    }


    public void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( CassandraClusterConfig.PRODUCT_NAME, config.getTEMPLATE_NAME(),
                Common.DEFAULT_DOMAIN_NAME, 1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );
        try
        {
            Set<ContainerHost> newNodeSet;
            try
            {
                newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
            }
            catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
            {
                throw new ClusterException( e );
            }

            ContainerHost newNode = newNodeSet.iterator().next();

            config.getNodes().add( newNode.getId() );

            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            try
            {
                configurator
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
            }
            catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
            {
                throw new ClusterException( e );
            }

            //subscribe to alerts
            try
            {
                manager.subscribeToAlerts( newNode );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }
            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
    }


    private ContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager ){
        ContainerHost unUsedContainerInEnvironment = null;
        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            Set<ContainerHost> containerHostSet = environment.getContainerHosts();
            for ( ContainerHost host : containerHostSet ){
                if ( ! config.getAllNodes().contains( host.getId() ) ){
                    unUsedContainerInEnvironment = host;
                    break;
                }
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return unUsedContainerInEnvironment;
    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Setting up cluster..." );

        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );

            new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }


    public void removeCluster()
    {
        CassandraClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        manager.getPluginDAO().deleteInfo( CassandraClusterConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster removed from database" );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            CommandResult result = null;
            switch ( clusterOperationType )
            {
                case START_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.startCommand );
                    }
                    break;
                case STOP_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.stopCommand );
                    }
                    break;
                case STATUS_ALL:
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.statusCommand );
                    }
                    break;
            }
            NodeOperationHandler.logResults( trackerOperation, result );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
        }
    }


    private CommandResult executeCommand( ContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", command );
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void destroyCluster()
    {
        CassandraClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
        }

        if ( manager.getPluginDAO().deleteInfo( CassandraClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }
    }
}
