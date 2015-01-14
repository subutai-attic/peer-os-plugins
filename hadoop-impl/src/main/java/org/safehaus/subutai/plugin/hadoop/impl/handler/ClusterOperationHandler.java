package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentDestroyException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;

import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


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
            case ADD:
                addNode();
                break;
        }
    }



    /**
     * Steps:
     *   1) Creates a new container from hadoop template
     *   2) Include node
     */
    public void addNode(){
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup();
        String nodeGroupName = HadoopClusterConfig.PRODUCT_NAME + "_" + System.currentTimeMillis();
        nodeGroup.setName( nodeGroupName );
        nodeGroup.setLinkHosts( true );
        nodeGroup.setExchangeSshKeys( true );
        nodeGroup.setDomainName( Common.DEFAULT_DOMAIN_NAME );
        nodeGroup.setTemplateName( HadoopClusterConfig.TEMPLATE_NAME );
        nodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );
        nodeGroup.setNumberOfNodes( 1 );

        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        String ngJSON = gson.toJson(nodeGroup);

        try
        {
            environmentManager.createAdditionalContainers( config.getEnvironmentId(), ngJSON, localPeer );
            Environment environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
            // update cluster configuration on DB
            Set<ContainerHost> newlyCreatedContainers = new HashSet<>();
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( ! config.getAllSlaveNodes().contains( containerHost.getId() ) ){
                    if ( containerHost.getNodeGroupName().equals( nodeGroupName ) ){
                        config.getDataNodes().add( containerHost.getId() );
                        config.getTaskTrackers().add( containerHost.getId() );
                        trackerOperation.addLog( containerHost.getHostname() + " is added as slave node." );
                        newlyCreatedContainers.add( containerHost );
                    }
                }
            }
            manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );

            // include newly created containers to existing hadoop cluster
            for ( ContainerHost containerHost : newlyCreatedContainers ){
                trackerOperation.addLog( "Configuring " + containerHost.getHostname()   );
                manager.includeNode( config, containerHost.getHostname()  );
            }
            trackerOperation.addLogDone( "Finished." );
        }
        catch ( EnvironmentBuildException e )
        {
            e.printStackTrace();
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
            Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
            ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
            ContainerHost jobtracker = environment.getContainerHostById( config.getJobTracker() );
            ContainerHost secondaryNameNode = environment.getContainerHostById( config.getSecondaryNameNode() );

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
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
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
                                     .buildEnvironment( manager.getDefaultEnvironmentBlueprint( config ) );
            ClusterSetupStrategy setupStrategy = manager.getClusterSetupStrategy( env, config, trackerOperation );
            setupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup Hadoop cluster %s : %s", clusterName, e.getMessage() ) );
        }
        catch ( EnvironmentBuildException e )
        {
            e.printStackTrace();
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
            manager.getEnvironmentManager().destroyEnvironment( config.getEnvironmentId() );
            manager.getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName() );
            trackerOperation.addLogDone( "Cluster destroyed" );
        }
        catch ( EnvironmentDestroyException e )
        {
            trackerOperation.addLogFailed( String.format( "Error running command, %s", e.getMessage() ) );
            LOG.error( e.getMessage(), e );
        }
    }
}
