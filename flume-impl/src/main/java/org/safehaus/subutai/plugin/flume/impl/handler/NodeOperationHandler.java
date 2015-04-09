package org.safehaus.subutai.plugin.flume.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.flume.api.FlumeConfig;
import org.safehaus.subutai.plugin.flume.impl.CommandType;
import org.safehaus.subutai.plugin.flume.impl.Commands;
import org.safehaus.subutai.plugin.flume.impl.FlumeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class NodeOperationHandler extends AbstractOperationHandler<FlumeImpl, FlumeConfig>
{

    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    private String clusterName;
    private String hostName;
    private NodeOperationType operationType;


    public NodeOperationHandler( final FlumeImpl manager, final String clusterName, final String hostName,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostName = hostName;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( FlumeConfig.PRODUCT_KEY,
                String.format( "Checking %s cluster...", clusterName ) );
    }


    @Override
    public void run()
    {
        FlumeConfig config = manager.getCluster( clusterName );
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
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId().toString(), e );
            return;
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        ContainerHost host = null;
        try
        {
            host = environment.getContainerHostByHostname( hostName );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostName ) );
            return;
        }

        try
        {
            CommandResult result;
            switch ( operationType )
            {
                case START:
                    result = host.execute( new RequestBuilder( Commands.make( CommandType.START ) ) );
                    logStatusResults( trackerOperation, result, operationType );
                    break;
                case STOP:
                    result = host.execute( new RequestBuilder( Commands.make( CommandType.STOP ) ) );
                    logStatusResults( trackerOperation, result, operationType );
                    break;
                case STATUS:
                    result = host.execute( new RequestBuilder( Commands.make( CommandType.SERVICE_STATUS ) ) );
                    logStatusResults( trackerOperation, result, operationType );
                    break;
                case INSTALL:
                    installProductOnNode( host );
                    break;
                case UNINSTALL:
                    uninstallProductOnNode( host );
                    break;
            }
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    private CommandResult installProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( new RequestBuilder( Commands.make( CommandType.INSTALL ) ).withTimeout( 600 ) );
            if ( result.hasSucceeded() )
            {
                config.getNodes().add( host.getId() );
                manager.getPluginDao().saveInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLogDone(
                        FlumeConfig.PRODUCT_KEY + " is installed on node " + host.getHostname() + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not install " + FlumeConfig.PRODUCT_KEY + " to node " + host.getHostname() );
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( new RequestBuilder( Commands.make( CommandType.PURGE ) ).withTimeout( 600 ) );
            if ( result.hasSucceeded() )
            {
                config.getNodes().remove( host.getId() );
                manager.getPluginDao().saveInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLogDone( FlumeConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                        + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not uninstall " + FlumeConfig.PRODUCT_KEY + " from node " + host.getHostname() );
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return result;
    }


    public static void logStatusResults( TrackerOperation po, CommandResult result, NodeOperationType operationType )
    {
        Preconditions.checkNotNull( result );
        StringBuilder log = new StringBuilder();
        String status;
        String output = result.getStdOut();
        if( operationType.equals( NodeOperationType.START ) && output.contains( "starting" ) )
        {
            status = "Flume is running";
        }
        else if( operationType.equals( NodeOperationType.STATUS ) && !result.getStdOut().equals( "" ))
        {
            status = "Flume is running";
        }
        else
        {
            status = "Flume is not running";
        }

        log.append( String.format( "%s", status ) );
        po.addLogDone( log.toString() );
    }
}
