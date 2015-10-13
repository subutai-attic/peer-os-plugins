package io.subutai.plugin.storm.impl.handler;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.CommandType;
import io.subutai.plugin.storm.impl.Commands;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.storm.impl.StormService;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * This class handles operations that are related to just one node.
 *
 * TODO: add nodes and delete node operation should be implemented.
 */
public class StormNodeOperationHandler extends AbstractOperationHandler<StormImpl, StormClusterConfiguration>
{
    private static final Logger LOG = LoggerFactory.getLogger( StormNodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;


    public StormNodeOperationHandler( final StormImpl manager, final String clusterName, final String hostname,
                                      NodeOperationType operationType )
    {
        super( manager, clusterName );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( StormClusterConfiguration.PRODUCT_NAME,
                String.format( "Running %s operation on %s...", operationType, hostname ) );
    }


    @Override
    public void run()
    {
        StormClusterConfiguration config = manager.getCluster( clusterName );
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
            logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId() ), e );
            return;
        }
        ContainerHost containerHost = null;
        try
        {
            containerHost = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by name: %s", hostname ), e );
        }
        // Check if the container is on external environment
        if ( config.isExternalZookeeper() && containerHost == null )
        {
            ZookeeperClusterConfig zookeeperCluster =
                    manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
            Environment zookeeperEnvironment;
            try
            {
                zookeeperEnvironment =
                        manager.getEnvironmentManager().loadEnvironment( zookeeperCluster.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException(
                        String.format( "Couldn't find environment by id: %s", zookeeperCluster.getEnvironmentId() ),
                        e );
                return;
            }
            try
            {
                containerHost = zookeeperEnvironment.getContainerHostByHostname( hostname );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Error getting container host by name: %s", hostname ), e );
                return;
            }
        }

        if ( containerHost == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }

        try
        {
            List<CommandResult> commandResultList = new ArrayList<>();
            switch ( operationType )
            {
                case START:
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost
                                .execute( new RequestBuilder( manager.getZookeeperManager().getCommand(

                                        io.subutai.plugin.zookeeper.api.CommandType.START ) ) ) );
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.START, StormService.NIMBUS ) ) ) );
                        commandResultList.add( containerHost
                                .execute( new RequestBuilder( Commands.make( CommandType.START, StormService.UI ) ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.START, StormService.SUPERVISOR ) ) ) );
                    }
                    break;
                case STOP:
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.STOP, StormService.NIMBUS ) ) ) );
                        commandResultList.add( containerHost
                                .execute( new RequestBuilder( Commands.make( CommandType.STOP, StormService.UI ) ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.STOP, StormService.SUPERVISOR ) ) ) );
                    }
                    break;
                case STATUS:
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.STATUS, StormService.NIMBUS ) ) ) );
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.STATUS, StormService.UI ) ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute(
                                new RequestBuilder( Commands.make( CommandType.STATUS, StormService.SUPERVISOR ) ) ) );
                    }
                    break;
                case DESTROY:
                    destroyNode();
                    break;
            }
            logResults( trackerOperation, commandResultList );
        }
        catch ( CommandException e )
        {
            logException( "Command failed", e );
        }
    }


    public void destroyNode()
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        StormClusterConfiguration config = manager.getCluster( clusterName );
        Environment environment;
        try
        {
            environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId() ), e );
            return;
        }
        ContainerHost host;
        try
        {
            host = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by name: %s", hostname ), e );
            return;
        }

        trackerOperation.addLog( "Removing " + hostname + " from cluster." );
        config.getSupervisors().remove( host.getId() );
        try
        {
            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated." );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error while saving cluster configuration." );
            e.printStackTrace();
        }
    }


    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
        {
            po.addLog( commandResult.getStdOut() );
        }
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
        else
        {
            po.addLogDone( "" );
        }
    }


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        trackerOperation.addLogFailed( msg );
    }
}
