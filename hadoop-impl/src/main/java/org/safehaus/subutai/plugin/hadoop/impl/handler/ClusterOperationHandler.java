package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.exception.EnvironmentCreationException;
import org.safehaus.subutai.core.env.api.exception.EnvironmentDestructionException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


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
    private ExecutorService executor = Executors.newCachedThreadPool();


    public ClusterOperationHandler( final HadoopImpl manager, final HadoopClusterConfig config,
                                    final ClusterOperationType operationType, NodeType nodeType )
    {
        super( manager, config.getClusterName() );
        this.operationType = operationType;
        this.config = config;
        this.nodeType = nodeType;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
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
            case REMOVE:
                removeCluster();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
            case DECOMISSION_STATUS:
                runOperationOnContainers( ClusterOperationType.DECOMISSION_STATUS );
                break;
        }
    }


    public void removeCluster()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
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
            ContainerHost namenode = null;
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
            ContainerHost jobtracker = null;
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
            ContainerHost secondaryNameNode = null;
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
                            result = namenode.execute( new RequestBuilder( Commands.getStartNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = jobtracker.execute( new RequestBuilder( Commands.getStartJobTrackerCommand() ) );
                            break;
                    }
                    logStatusResults( trackerOperation, result, nodeType );
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
                    logStatusResults( trackerOperation, result, nodeType );
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
                    logStatusResults( trackerOperation, result, nodeType );
                    break;
                case DECOMISSION_STATUS:
                    result = namenode.execute( new RequestBuilder( Commands.getReportHadoopCommand() ) );
                    logStatusResults( trackerOperation, result, NodeType.SLAVE_NODE );
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


    public static void logStatusResults( TrackerOperation trackerOperation, CommandResult result, NodeType nodeType )
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
            trackerOperation.addLogFailed( String.format( "Failed to check status of node" ) );
        }
        else
        {
            trackerOperation.addLogDone( String.format( "Node state is %s", nodeState ) );
        }
    }


    @Override
    public void setupCluster()
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            trackerOperation.addLogFailed( "Malformed configuration" );
            return;
        }

        if ( manager.getCluster( clusterName ) != null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name '%s' already exists", clusterName ) );
            return;
        }

        try
        {
            Environment env = manager.getEnvironmentManager()
                                     .createEnvironment( config.getClusterName(), config.getTopology(), false );
            ClusterSetupStrategy setupStrategy = manager.getClusterSetupStrategy( env, config, trackerOperation );
            setupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( ClusterSetupException e )
        {
            logExceptionWithMessage( String.format( "Failed to setup Hadoop cluster %s", clusterName ), e );
        }
        catch ( EnvironmentCreationException e )
        {
            logExceptionWithMessage( "Error creating environment with name: " + config.getClusterName(), e );
        }
    }


    @Override
    public void destroyCluster()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );

        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Installation with name %s does not exist", clusterName ) );
            return;
        }

        try
        {
            trackerOperation.addLog( "Destroying environment..." );
            manager.getEnvironmentManager().destroyEnvironment( config.getEnvironmentId(), true, true );
            manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() );
            trackerOperation.addLogDone( "Cluster destroyed" );
        }
        catch ( EnvironmentNotFoundException | EnvironmentDestructionException e )
        {
            logExceptionWithMessage( "Error performing operations on environment", e );
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOG.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
