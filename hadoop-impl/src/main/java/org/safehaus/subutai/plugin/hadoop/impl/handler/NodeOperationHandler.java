package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    private ExecutorService executor = Executors.newCachedThreadPool();


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
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            Iterator iterator = environment.getContainerHosts().iterator();
            ContainerHost host = null;
            while ( iterator.hasNext() )
            {
                host = ( ContainerHost ) iterator.next();
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


    protected void runCommand( ContainerHost host, NodeOperationType operationType, NodeType nodeType )
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
                            result = host.execute( new RequestBuilder( Commands.getStartNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStartJobTrackerCommand() ) );
                            break;
                        case TASKTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStartTaskTrackerCommand() ) );
                            break;
                        case DATANODE:
                            result = host.execute( new RequestBuilder( Commands.getStartDataNodeCommand() ) );
                            break;
                    }
                    ClusterOperationHandler.logStatusResults( trackerOperation, result, nodeType );
                    break;
                case STOP:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = host.execute( new RequestBuilder( Commands.getStopNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStopJobTrackerCommand() ) );
                            break;
                        case TASKTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStopTaskTrackerCommand() ) );
                            break;
                        case DATANODE:
                            result = host.execute( new RequestBuilder( Commands.getStopDataNodeCommand() ) );
                            break;
                    }
                    ClusterOperationHandler.logStatusResults( trackerOperation, result, nodeType );
                    break;
                case STATUS:
                    switch ( nodeType )
                    {
                        case NAMENODE:
                            result = host.execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ) );
                            break;
                        case JOBTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStatusJobTrackerCommand() ) );
                            break;
                        case SECONDARY_NAMENODE:
                            result = host.execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ) );
                            break;
                        case TASKTRACKER:
                            result = host.execute( new RequestBuilder( Commands.getStatusTaskTrackerCommand() ) );
                            break;
                        case DATANODE:
                            result = host.execute( new RequestBuilder( Commands.getStatusDataNodeCommand() ) );
                            break;
                    }
                    ClusterOperationHandler.logStatusResults( trackerOperation, result, nodeType );
                    break;
                case EXCLUDE:
                    excludeNode();
                    break;
                case INCLUDE:
                    includeNode();
                    break;
            }
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Command failed", e );
        }
    }


    protected void excludeNode()
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        ContainerHost host = findNodeInCluster( hostname );

        try
        {
            // TaskTracker
            host.execute( new RequestBuilder( Commands.getRemoveTaskTrackerCommand( host.getHostname() ) ) );
            host.execute( new RequestBuilder(
                    Commands.getIncludeTaskTrackerCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // DataNode
            host.execute( new RequestBuilder( Commands.getRemoveDataNodeCommand( host.getHostname() ) ) );
            host.execute(
                    new RequestBuilder( Commands.getIncludeDataNodeCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            // refresh NameNode and JobTracker
            ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
            ContainerHost jobtracker = environment.getContainerHostById( config.getJobTracker() );

            namenode.execute( new RequestBuilder( Commands.getRefreshNameNodeCommand() ) );
            jobtracker.execute( new RequestBuilder( Commands.getRefreshJobTrackerCommand() ) );
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage(
                    String.format( "Error running commands {%s} {%s} on nodes", Commands.getRefreshJobTrackerCommand(),
                            Commands.getRefreshNameNodeCommand() ), e );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            if ( ( e instanceof EnvironmentNotFoundException ) )
            {
                logExceptionWithMessage(
                        String.format( "Couldn't get environment with id: %s", config.getEnvironmentId().toString() ),
                        e );
            }
            else
            {
                logExceptionWithMessage( String.format( "Couldn't find one of containers" ), e );
            }
        }

        config.getBlockedAgents().add( host.getId() );
        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        trackerOperation.addLogDone( "Cluster info saved to DB" );
    }


    protected ContainerHost findNodeInCluster( String hostname )
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

            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            Iterator iterator = environment.getContainerHosts().iterator();

            ContainerHost host = null;
            while ( iterator.hasNext() )
            {
                host = ( ContainerHost ) iterator.next();
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
        ContainerHost host = findNodeInCluster( hostname );

        try
        {
            ContainerHost namenode = null;
            try
            {
                namenode = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                  .getContainerHostById( config.getNameNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage( String.format( "Name node container host with id: %s not found",
                        config.getNameNode().toString() ), e );
                return;
            }
            catch ( EnvironmentNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Environment with id: %s not found", config.getEnvironmentId().toString() ), e );
                return;
            }

            ContainerHost jobtracker = null;
            try
            {
                jobtracker = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                    .getContainerHostById( config.getJobTracker() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage( String.format( "Job tracker container host with id: %s not found",
                        config.getJobTracker().toString() ), e );
                return;
            }
            catch ( EnvironmentNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Environment with id: %s not found", config.getEnvironmentId().toString() ), e );
                return;
            }

            /** DataNode Operations */
            // set data node
            namenode.execute( new RequestBuilder( Commands.getSetDataNodeCommand( host.getHostname() ) ) );

            // remove data node from dfs.exclude
            namenode.execute(
                    new RequestBuilder( Commands.getExcludeDataNodeCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // stop data node
            host.execute( new RequestBuilder( Commands.getStopDataNodeCommand() ) );

            // start data node
            host.execute( new RequestBuilder( Commands.getStartDataNodeCommand() ) );

            // refresh name node
            namenode.execute( new RequestBuilder( Commands.getRefreshNameNodeCommand() ) );


            /** TaskTracker Operations */
            // set task tracker
            jobtracker.execute( new RequestBuilder( Commands.getSetTaskTrackerCommand( host.getHostname() ) ) );

            // remove task tracker from dfs.exclude
            jobtracker.execute( new RequestBuilder(
                    Commands.getExcludeTaskTrackerCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // stop task tracker
            host.execute( new RequestBuilder( Commands.getStopTaskTrackerCommand() ) );

            // start task tracker
            host.execute( new RequestBuilder( Commands.getStartTaskTrackerCommand() ) );

            // refresh job tracker
            jobtracker.execute( new RequestBuilder( Commands.getRefreshJobTrackerCommand() ) );
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Error running command", e );
        }

        config.getBlockedAgents().remove( host.getId() );
        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        trackerOperation.addLogDone( "Cluster info saved to DB" );
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
