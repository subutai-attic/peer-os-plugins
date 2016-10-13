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
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.ClusterConfiguration;
import io.subutai.plugin.storm.impl.CommandType;
import io.subutai.plugin.storm.impl.Commands;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.storm.impl.StormService;


public class StormNodeOperationHandler extends AbstractOperationHandler<StormImpl, StormClusterConfiguration>
{
    private static final Logger LOG = LoggerFactory.getLogger( StormNodeOperationHandler.class );
    private String clusterName;
    private String nodeId;
    private NodeOperationType operationType;


    public StormNodeOperationHandler( final StormImpl manager, final String clusterName, final String nodeId,
                                      NodeOperationType operationType )
    {
        super( manager, clusterName );
        this.nodeId = nodeId;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( StormClusterConfiguration.PRODUCT_NAME,
                String.format( "Running %s operation on %s...", operationType, nodeId ) );
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
            containerHost = environment.getContainerHostById( nodeId );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by id: %s", nodeId ), e );
        }

        if ( containerHost == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", nodeId ) );
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
                        commandResultList.add( containerHost.execute( Commands.getStartNimbusCommand() ) );
                        commandResultList.add( containerHost.execute( Commands.getStartStormUICommand() ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute( Commands.getStartSupervisorCommand() ) );
                    }
                    containerHost.execute( new RequestBuilder( "sleep 10" ) );
                    break;
                case STOP:
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute( Commands.getStopNimbusCommand() ) );
                        commandResultList.add( containerHost.execute( Commands.getStopStormUICommand() ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( containerHost.execute( Commands.getStopSupervisorCommand() ) );
                    }
                    containerHost.execute( new RequestBuilder( "sleep 5" ) );
                    break;
                case STATUS:
                    commandResultList.add( containerHost.execute( Commands.getStatusCommand() ) );
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

        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            ContainerHost host = environment.getContainerHostById( nodeId );

            ClusterConfiguration configuration = new ClusterConfiguration( trackerOperation, manager );
            configuration.removeNode( host, config, environment );

            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated." );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logException( String.format( "Couldn't find environment by id: %s", config.getEnvironmentId() ), e );
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by id: %s", nodeId ), e );
            e.printStackTrace();
        }
        catch ( ClusterException e )
        {
            logException( "Error while saving cluster configuration.", e );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            logException( "Error while executing command.", e );
            e.printStackTrace();
        }
        catch ( ClusterConfigurationException e )
        {
            logException( "Error while reconfiguring cluster.", e );
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
