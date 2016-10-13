package io.subutai.plugin.hadoop.impl.handler;


import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.ClusterConfiguration;
import io.subutai.plugin.hadoop.impl.Commands;
import io.subutai.plugin.hadoop.impl.HadoopImpl;


/**
 * This class handles operations that are related to just one node.
 *
 * TODO: add nodes and delete node operation should be implemented.
 */
public class NodeOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NodeType nodeType;


    public NodeOperationHandler( final HadoopImpl manager, final String clusterName, final String hostname,
                                 NodeOperationType operationType, NodeType nodeType )
    {
        super( manager, clusterName );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            Iterator<EnvironmentContainerHost> iterator = environment.getContainerHosts().iterator();
            EnvironmentContainerHost host = null;
            while ( iterator.hasNext() )
            {
                host = iterator.next();
                if ( host.getHostname().equals( hostname ) )
                {
                    break;
                }
            }

            if ( host == null )
            {
                trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
                return;
            }
            runCommand( host, operationType, nodeType );
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage( "Couldn't retrieve environment", e );
        }
    }


    void runCommand( EnvironmentContainerHost host, NodeOperationType operationType, NodeType nodeType )
    {
        try
        {
            CommandResult result = null;
            switch ( operationType )
            {
                case START:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            host.execute( new RequestBuilder( Commands.getStartDfsCommand() ).withTimeout( 20 ) );
                            result = host.execute(
                                    new RequestBuilder( Commands.getStartYarnCommand() ).withTimeout( 20 ) );
                            break;
                    }
                    break;
                case STOP:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            host.execute( new RequestBuilder( Commands.getStopDfsCommand() ).withTimeout( 20 ) );
                            result = host.execute(
                                    new RequestBuilder( Commands.getStopYarnCommand() ).withTimeout( 20 ) );
                            break;
                    }
                    break;
                case STATUS:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = host.execute( new RequestBuilder( Commands.getNodeStatusCommand() ) );
                            break;
                        case DATANODE:
                            result = host.execute( new RequestBuilder( Commands.getNodeStatusCommand() ) );
                            break;
                    }
                    break;
                case EXCLUDE:
                    excludeNode();
                    break;
                case INCLUDE:
                    includeNode();
                    break;
            }
            logResults( trackerOperation, result );
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Command failed", e );
        }
    }


    protected void excludeNode()
    {
        try
        {
            HadoopClusterConfig config = manager.getCluster( clusterName );
            EnvironmentContainerHost host = findNodeInCluster( hostname );
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

            config.getSlaves().remove( host.getId() );
            config.getExcludedSlaves().add( host.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() );

            EnvironmentContainerHost namenode = environment.getContainerHostById( config.getNameNode() );

            ClusterConfiguration configurator = new ClusterConfiguration( trackerOperation, manager );
            configurator.excludeNode( namenode, host, config, environment );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            if ( ( e instanceof EnvironmentNotFoundException ) )
            {
                logExceptionWithMessage(
                        String.format( "Couldn't get environment with id: %s", config.getEnvironmentId() ), e );
            }
            else
            {
                logExceptionWithMessage( String.format( "Couldn't find one of containers" ), e );
            }
        }

        //        config.getBlockedAgents().add( host.getId() );
        //        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        //        trackerOperation.addLogDone( "Cluster info saved to DB" );
    }


    protected EnvironmentContainerHost findNodeInCluster( String hostname )
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );

        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Installation with name %s does not exist", clusterName ) );
            return null;
        }

        if ( config.getNameNode() == null )
        {
            trackerOperation.addLogFailed( String.format( "NameNode on %s does not exist", clusterName ) );
            return null;
        }

        try
        {

            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            Iterator iterator = environment.getContainerHosts().iterator();

            EnvironmentContainerHost host = null;
            while ( iterator.hasNext() )
            {
                host = ( EnvironmentContainerHost ) iterator.next();
                if ( host.getHostname().equals( hostname ) )
                {
                    break;
                }
            }

            if ( host == null )
            {
                trackerOperation.addLogFailed( String.format( "No Container Host" ) );
                return null;
            }
            return host;
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage( "Error getting environment", e );
            return null;
        }
    }


    protected void includeNode()
    {

        HadoopClusterConfig config = manager.getCluster( clusterName );
        EnvironmentContainerHost host = findNodeInCluster( hostname );

        EnvironmentContainerHost namenode;
        try
        {
            namenode = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                              .getContainerHostById( config.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logExceptionWithMessage(
                    String.format( "Name node container host with id: %s not found", config.getNameNode() ), e );
            return;
        }
        catch ( EnvironmentNotFoundException e )
        {
            logExceptionWithMessage( String.format( "Environment with id: %s not found", config.getEnvironmentId() ),
                    e );
            return;
        }

        EnvironmentContainerHost jobtracker;
        //                jobtracker = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
        //                                    .getContainerHostById( config.getJobTracker() );

        /** DataNode Operations */
        // set data node
        //            namenode.execute( new RequestBuilder( Commands.getSetDataNodeCommand( host.getHostname() ) ) );

        // remove data node from dfs.exclude
        //            namenode.execute( new RequestBuilder(
        //                    Commands.getExcludeDataNodeCommand( host.getInterfaceByName( "eth0" ).getIp() ) ) );

        // start datanode if namenode is already running

        /** TaskTracker Operations */
        // set task tracker
        // check if namenode and jobtracker are on different containers
        //            if ( !namenode.getId().equals( jobtracker.getId() ) )
        //            {
        //                jobtracker.execute( new RequestBuilder( Commands.getSetTaskTrackerCommand( host.getHostname
        // () ) ) );

        //            }

        // remove task tracker from dfs.exclude
        //            jobtracker.execute( new RequestBuilder(
        //                    Commands.getExcludeTaskTrackerCommand( host.getInterfaceByName( "eth0" ).getIp() ) ) );

        // start tasktracker if namenode is already running

        //        config.getBlockedAgents().remove( host.getId() );
        //        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        //        trackerOperation.addLogDone( "Cluster info saved to DB" );
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
        trackerOperation.addLogFailed( message );
    }


    private static void logResults( TrackerOperation po, CommandResult result )
    {
        if ( result != null )
        {
            StringBuilder log = new StringBuilder();
            String status;
            if ( result.getExitCode() == 0 )
            {
                status = result.getStdOut();
            }
            else if ( result.getExitCode() == 768 )
            {
                status = "hadoop is not running";
            }
            else
            {
                status = result.getStdOut();
            }
            log.append( String.format( "%s", status ) );
            po.addLogDone( log.toString() );
        }
    }
}
