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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
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
                    ClusterOperationHandler.logResults( trackerOperation, result, nodeType );
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
                    ClusterOperationHandler.logResults( trackerOperation, result, nodeType );
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
                    ClusterOperationHandler.logResults( trackerOperation, result, nodeType );
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
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

            // refresh NameNode and JobTracker
            ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
            ContainerHost jobtracker = environment.getContainerHostById( config.getJobTracker() );

            // TaskTracker
            jobtracker.execute( new RequestBuilder( Commands.getRemoveTaskTrackerCommand( host.getHostname() ) ) );
            jobtracker.execute( new RequestBuilder(
                    Commands.getIncludeTaskTrackerCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // DataNode
            namenode.execute( new RequestBuilder( Commands.getRemoveDataNodeCommand( host.getHostname() ) ) );
            namenode.execute(
                    new RequestBuilder( Commands.getIncludeDataNodeCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

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
                        String.format( "Couldn't get environment with id: %s", config.getEnvironmentId() ), e );
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
        ContainerHost host = findNodeInCluster( hostname );

        try
        {
            ContainerHost namenode;
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
                logExceptionWithMessage(
                        String.format( "Environment with id: %s not found", config.getEnvironmentId() ), e );
                return;
            }

            ContainerHost jobtracker;
            try
            {
                jobtracker = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                    .getContainerHostById( config.getJobTracker() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Job tracker container host with id: %s not found", config.getJobTracker() ),
                        e );
                return;
            }
            catch ( EnvironmentNotFoundException e )
            {
                logExceptionWithMessage(
                        String.format( "Environment with id: %s not found", config.getEnvironmentId() ), e );
                return;
            }

            /** DataNode Operations */
            // set data node
            namenode.execute( new RequestBuilder( Commands.getSetDataNodeCommand( host.getHostname() ) ) );

            // remove data node from dfs.exclude
            namenode.execute(
                    new RequestBuilder( Commands.getExcludeDataNodeCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // start datanode if namenode is already running
            if ( isClusterRunning( namenode ) )
            {
                // stop data node
                host.execute( new RequestBuilder( Commands.getStopDataNodeCommand() ) );

                // start data node
                host.execute( new RequestBuilder( Commands.getStartDataNodeCommand() ) );

                // refresh name node
                namenode.execute( new RequestBuilder( Commands.getRefreshNameNodeCommand() ) );
            }

            /** TaskTracker Operations */
            // set task tracker
            // check if namenode and jobtracker are on different containers
            if ( !namenode.getId().equals( jobtracker.getId() ) )
            {
                jobtracker.execute( new RequestBuilder( Commands.getSetTaskTrackerCommand( host.getHostname() ) ) );
            }

            // remove task tracker from dfs.exclude
            jobtracker.execute( new RequestBuilder(
                    Commands.getExcludeTaskTrackerCommand( host.getIpByInterfaceName( "eth0" ) ) ) );

            // start tasktracker if namenode is already running
            if ( isClusterRunning( namenode ) )
            {
                // stop task tracker
                host.execute( new RequestBuilder( Commands.getStopTaskTrackerCommand() ) );

                // start task tracker
                host.execute( new RequestBuilder( Commands.getStartTaskTrackerCommand() ) );

                // refresh job tracker
                jobtracker.execute( new RequestBuilder( Commands.getRefreshJobTrackerCommand() ) );
            }
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Error running command", e );
        }

        config.getBlockedAgents().remove( host.getId() );
        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        trackerOperation.addLogDone( "Cluster info saved to DB" );
    }


    private boolean isClusterRunning( ContainerHost namenode )
    {
        try
        {
            CommandResult result = namenode.execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ) );
            if ( result.hasSucceeded() )
            {
                if ( result.getStdOut().toLowerCase().contains( "pid" ) )
                {
                    return true;
                }
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return false;
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
