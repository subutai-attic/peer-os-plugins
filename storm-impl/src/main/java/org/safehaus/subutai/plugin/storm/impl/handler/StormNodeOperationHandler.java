package org.safehaus.subutai.plugin.storm.impl.handler;


import java.util.ArrayList;
import java.util.List;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.CommandType;
import org.safehaus.subutai.plugin.storm.impl.Commands;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.safehaus.subutai.plugin.storm.impl.StormService;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


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

        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId().toString() ),
                    e );
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
            return;
        }
        // Check if the container is on external environment
        if ( config.isExternalZookeeper() && containerHost == null )
        {
            ZookeeperClusterConfig zookeeperCluster =
                    manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
            Environment zookeeperEnvironment = null;
            try
            {
                zookeeperEnvironment =
                        manager.getEnvironmentManager().findEnvironment( zookeeperCluster.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Couldn't find environment by id: %s",
                        zookeeperCluster.getEnvironmentId().toString() ), e );
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

                                        org.safehaus.subutai.plugin.zookeeper.api.CommandType.START ) ) ) );
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
        Environment environment = null;
        try
        {
            environment = environmentManager.findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId().toString() ),
                    e );
            return;
        }
        ContainerHost host = null;
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
        manager.getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config );
        trackerOperation.addLog( "Destroying " + hostname + " node." );
        //            environmentManager.removeContainer( environment.getId(), environment.getContainerHostByHostname
        // ( hostname ).getId() );
        trackerOperation.addLogDone( "Container " + hostname + " is destroyed!" );
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
