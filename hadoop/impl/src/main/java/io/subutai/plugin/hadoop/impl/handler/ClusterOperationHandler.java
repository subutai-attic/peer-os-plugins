package io.subutai.plugin.hadoop.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.peer.PeerException;
import io.subutai.common.protocol.CustomProxyConfig;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.ClusterConfiguration;
import io.subutai.plugin.hadoop.impl.Commands;
import io.subutai.plugin.hadoop.impl.HadoopImpl;


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
                String.format( "Starting %s operation on %s(%s) cluster...", operationType, clusterName,
                        config.getProductKey() ) );
    }


    public ClusterOperationHandler( final HadoopImpl manager, final HadoopClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        Preconditions.checkState( operationType.equals( ClusterOperationType.INSTALL ) );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Starting %s operation on %s(%s) cluster...", operationType, clusterName,
                        config.getProductKey() ) );
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
        }
    }


    public void removeCluster()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        // before removing cluster, stop it first.
        //        manager.stopNameNode( config );

        // clear "/var/lib/hadoop-root" directories
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            CommandUtil commandUtil = new CommandUtil();
            Set<Host> hostSet = new HashSet<>();
            for ( EnvironmentContainerHost host : environment.getContainerHosts() )
            {
                hostSet.add( host );
            }
            //            commandUtil.execute( new RequestBuilder( Commands.getClearDataDirectory() ), hostSet,
            // environment.getId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster removed from database" );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            EnvironmentContainerHost namenode;
            try
            {
                namenode = ( EnvironmentContainerHost ) environment.getContainerHostById( config.getNameNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage( String.format( "Container host with id: %s not found", config.getNameNode() ),
                        e );
                return;
            }

            CommandResult result = null;
            switch ( clusterOperationType )
            {
                case START_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            //                            result = namenode.execute(
                            //                                    new RequestBuilder( Commands
                            // .getStartNameNodeCommand() ).daemon() );
                            break;
                        //                        case JOBTRACKER:
                        //                            result = jobtracker
                        //                                    .execute( new RequestBuilder( Commands
                        // .getStartJobTrackerCommand() ).daemon() );
                        //                            break;
                    }
                    assert result != null;
                    if ( result.hasSucceeded() )
                    {
                        trackerOperation.addLogDone( result.getStdOut() );
                    }
                    else
                    {
                        trackerOperation.addLogFailed( result.getStdErr() );
                    }
                    break;
                case STOP_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            //                            result = namenode.execute( new RequestBuilder( Commands
                            // .getStopNameNodeCommand() ) );
                            break;
                        //                        case JOBTRACKER:
                        //                            result = jobtracker.execute( new RequestBuilder( Commands
                        // .getStopJobTrackerCommand() ) );
                        //                            break;
                    }
                    assert result != null;
                    if ( result.hasSucceeded() )
                    {
                        trackerOperation.addLogDone( result.getStdOut() );
                    }
                    else
                    {
                        trackerOperation.addLogFailed( result.getStdErr() );
                    }
                    break;
                case STATUS_ALL:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            //                            result = namenode.execute( new RequestBuilder( Commands
                            // .getStatusNameNodeCommand() ) );
                            break;
                        //                        case JOBTRACKER:
                        //                            result = jobtracker.execute( new RequestBuilder( Commands
                        // .getStatusJobTrackerCommand() ) );
                        //                            break;
                        //                        case SECONDARY_NAMENODE:
                        //                            result = secondaryNameNode
                        //                                    .execute( new RequestBuilder( Commands
                        // .getStatusNameNodeCommand() ) );
                        //                            break;
                    }
                    logResults( trackerOperation, result, nodeType );
                    break;
                case DECOMISSION_STATUS:
                    //                    result = namenode.execute( new RequestBuilder( Commands
                    // .getReportHadoopCommand() ) );
                    logResults( trackerOperation, result, NodeType.SLAVE_NODE );
                    break;
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage( String.format( "Environment with id: %s not found", config.getEnvironmentId() ),
                    e );
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
                            nodeState = NodeState.RUNNING;
                        }
                        else
                        {
                            nodeState = NodeState.STOPPED;
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
            Environment env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            trackerOperation.addLog( String.format( "Configuring %s environment for %s(%s) cluster", env.getName(),
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
            trackerOperation
                    .addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            HadoopClusterConfig config = manager.getCluster( clusterName );

            if ( config == null )
            {
                trackerOperation.addLogFailed(
                        String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
                return;
            }

            new ClusterConfiguration( trackerOperation, manager ).deleteClusterConfiguration( config, environment );

            if ( manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
            {
                trackerOperation.addLogDone( "Cluster information deleted from database" );

                EnvironmentContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
                CustomProxyConfig proxyConfig =
                        new CustomProxyConfig( config.getVlan(), config.getNameNode(), config.getEnvironmentId() );
                namenode.getPeer().removeCustomProxy( proxyConfig );
            }
            else
            {
                trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
                LOG.error( "Failed to delete cluster information from database" );
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            LOG.error( "Environment not found" );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "Container not found" );
            LOG.error( "Container not found" );
        }
        catch ( PeerException e )
        {
            LOG.error( "Error to delete proxy settings: ", e );
            trackerOperation.addLogFailed( "Error to delete proxy settings." );
            e.printStackTrace();
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOG.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
