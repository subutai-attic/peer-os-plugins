package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private HadoopClusterConfig config;
    private NodeType nodeType;


    public ClusterOperationHandler( final HadoopImpl manager, final HadoopClusterConfig config,
                                    final ClusterOperationType operationType, NodeType nodeType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.nodeType = nodeType;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Starting %s operation on %s(%s) cluster...",
                        operationType, clusterName, config.getProductKey() ) );
    }


    public ClusterOperationHandler( final HadoopImpl manager, final HadoopClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        Preconditions.checkState( operationType.equals( ClusterOperationType.INSTALL ) );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Starting %s operation on %s(%s) cluster...",
                        operationType, clusterName, config.getProductKey() ) );
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
            case REMOVE:
                removeCluster();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
            case DECOMISSION_STATUS:
                runOperationOnContainers( operationType );
                break;
        }
    }


    public void removeCluster()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        // before removing cluster, stop it first.
        manager.stopNameNode( config );
        manager.stopJobTracker( config );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster removed from database" );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            ContainerHost namenode;
            try
            {
                namenode = environment.getContainerHostById( config.getNameNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Container host with id: %s not found", config.getNameNode().toString() ), e );
                return;
            }
            ContainerHost jobtracker;
            try
            {
                jobtracker = environment.getContainerHostById( config.getJobTracker() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Container host with id: %s not found", config.getJobTracker().toString() ), e );
                return;
            }
            ContainerHost secondaryNameNode;
            try
            {
                secondaryNameNode = environment.getContainerHostById( config.getSecondaryNameNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage( String.format( "Container host with id: %s not found",
                        config.getSecondaryNameNode().toString() ), e );
                return;
            }

            CommandResult result = null;
            switch ( clusterOperationType )
            {
                case START_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = namenode.execute(
                                    new RequestBuilder( Commands.getStartNameNodeCommand() ).daemon() );
                            break;
                        case JOBTRACKER:
                            result = jobtracker.execute( new
                                    RequestBuilder( Commands.getStartJobTrackerCommand() ).daemon() );
                            break;
                    }
                    assert result != null;
                    if ( result.hasSucceeded() ){
                        trackerOperation.addLogDone( result.getStdOut() );
                    } else{
                        trackerOperation.addLogFailed( result.getStdErr() );
                    }
                    break;
                case STOP_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = namenode.execute( new RequestBuilder( Commands.getStopNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = jobtracker.execute( new RequestBuilder( Commands.getStopJobTrackerCommand() ) );
                            break;
                    }
                    assert result != null;
                    if ( result.hasSucceeded() ){
                        trackerOperation.addLogDone( result.getStdOut() );
                    } else{
                        trackerOperation.addLogFailed( result.getStdErr() );
                    }
                    break;
                case STATUS_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = namenode.execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = jobtracker.execute( new RequestBuilder( Commands.getStatusJobTrackerCommand() ) );
                            break;
                        case SECONDARY_NAMENODE:
                            result = secondaryNameNode
                                    .execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ) );
                            break;
                    }
                    logResults( trackerOperation, result, nodeType );
                    break;
                case DECOMISSION_STATUS:
                    result = namenode.execute( new RequestBuilder( Commands.getReportHadoopCommand() ) );
                    logResults( trackerOperation, result, NodeType.SLAVE_NODE );
                    break;
            }
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Command failed", e );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage(
                    String.format( "Environment with id: %s not found", config.getEnvironmentId().toString() ), e );
        }
    }


    public static void logResults( TrackerOperation trackerOperation, CommandResult result, NodeType nodeType )
    {
        NodeState nodeState = NodeState.UNKNOWN;
        if ( result.getStdOut() != null )
        {
            String[] array = result.getStdOut().split( "\n" );

            for ( String status : array )
            {
                switch ( nodeType )
                {
                    case NAMENODE:
                        if ( status.contains( "NameNode" ) )
                        {
                            String temp = status.replaceAll(
                                    Pattern.quote( "!(SecondaryNameNode is not running on this " + "machine)" ), "" ).
                                                        replaceAll( "NameNode is ", "" );

                            if ( ! temp.toLowerCase().contains( "secondary" ) ){
                                if ( temp.toLowerCase().contains( "not" ) )
                                {
                                    nodeState = NodeState.STOPPED;
                                }
                                else
                                {
                                    nodeState = NodeState.RUNNING;
                                }
                                break;
                            }
                            break;
                        }
                        break;
                    case JOBTRACKER:
                        if ( status.contains( "JobTracker" ) )
                        {
                            String temp = status.replaceAll( "JobTracker is ", "" );
                            if ( temp.toLowerCase().contains( "not" ) )
                            {
                                nodeState = NodeState.STOPPED;
                            }
                            else
                            {
                                nodeState = NodeState.RUNNING;
                            }
                            break;
                        }
                        break;
                    case SECONDARY_NAMENODE:
                        if ( status.contains( "SecondaryNameNode" ) )
                        {
                            String temp = status.replaceAll( "SecondaryNameNode is ", "" );
                            if ( temp.toLowerCase().contains( "not" ) )
                            {
                                nodeState = NodeState.STOPPED;
                            }
                            else
                            {
                                nodeState = NodeState.RUNNING;
                            }
                        }
                        break;
                    case DATANODE:
                        if ( status.contains( "DataNode" ) )
                        {
                            String temp = status.replaceAll( "DataNode is ", "" );
                            if ( temp.toLowerCase().contains( "not" ) )
                            {
                                nodeState = NodeState.STOPPED;
                            }
                            else
                            {
                                nodeState = NodeState.RUNNING;
                            }
                        }
                        break;
                    case TASKTRACKER:
                        if ( status.contains( "TaskTracker" ) )
                        {
                            String temp = status.replaceAll( "TaskTracker is ", "" );
                            if ( temp.toLowerCase().contains( "not" ) )
                            {
                                nodeState = NodeState.STOPPED;
                            }
                            else
                            {
                                nodeState = NodeState.RUNNING;
                            }
                        }
                        break;
                    case SLAVE_NODE:
                        //                        nodeState = this.getDecommissionStatus( result.getStdOut() );
                        trackerOperation.addLogDone( result.getStdOut() );
                        break;
                }
            }
        }

        if ( NodeState.UNKNOWN.equals( nodeState ) )
        {
            trackerOperation.addLogFailed( String.format( "Unknown node state !!!" ) );
        }
        else
        {
            trackerOperation.addLogDone( String.format( "Node state is %s", nodeState ) );
        }
    }


    @Override
    public void setupCluster()
    {
        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            trackerOperation.addLog( String.format( "Configuring %s environment for %s(%s) cluster",env.getName(),
                    config.getClusterName(), config.getProductKey() ) );
            try
            {
                new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        // before removing cluster, stop it first.
        manager.stopNameNode( config );
        manager.stopJobTracker( config );

        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        Environment environment;
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

        if ( manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOG.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
