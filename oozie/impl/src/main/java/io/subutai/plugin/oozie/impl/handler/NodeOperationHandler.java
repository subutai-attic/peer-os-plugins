package io.subutai.plugin.oozie.impl.handler;


import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;


public class NodeOperationHandler extends AbstractOperationHandler<OozieImpl, OozieClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class );
    private String clusterName;
    private String hostName;
    private NodeOperationType operationType;


    public NodeOperationHandler( final OozieImpl manager, final String clusterName, final String hostName,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostName = hostName;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( OozieClusterConfig.PRODUCT_KEY,
                String.format( "Checking %s cluster...", clusterName ) );
    }


    @Override
    public void run()
    {
        OozieClusterConfig config = manager.getCluster( clusterName );
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
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
            return;
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Could not find cluster environment" );
            return;
        }

        Iterator iterator = environment.getContainerHosts().iterator();
        ContainerHost host = null;
        while ( iterator.hasNext() )
        {
            host = ( ContainerHost ) iterator.next();
            if ( host.getHostname().equals( hostName ) )
            {
                break;
            }
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
                    result = host.execute( Commands.getStartServerCommand() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STOP:
                    result = host.execute( Commands.getStopServerCommand() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STATUS:
                    result = host.execute( Commands.getStatusServerCommand() );
                    logStatusResults( trackerOperation, result );
                    break;
                case INSTALL:
                    result = installProductOnNode( host );
                    logStatusResults( trackerOperation, result );
                    break;
                case UNINSTALL:
                    result = uninstallProductOnNode( host );
                    logStatusResults( trackerOperation, result );
                    break;
            }
            //logStatusResults( trackerOperation, result );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    private CommandResult installProductOnNode( ContainerHost host )
    {
        //        CommandResult result = null;
        //        try
        //        {
        //            host.execute( Commands.getAptUpdate() );
        //            result = host.execute( new RequestBuilder( Commands.make( CommandType.INSTALL_CLIENT ) ) );
        //            if ( result.hasSucceeded() )
        //            {
        //                config.getClients().add( host.getId() );
        //                manager.getPluginDao().saveInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName(),
        // config );
        //                trackerOperation.addLogDone(
        //                        OozieClusterConfig.PRODUCT_KEY + " is installed on node " + host.getHostname() + " " +
        //                                "successfully." );
        //            }
        //            else
        //            {
        //                trackerOperation.addLogFailed(
        //                        "Could not install " + OozieClusterConfig.PRODUCT_KEY + " to node " + host
        // .getHostname() );
        //            }
        //        }
        //        catch ( CommandException e )
        //        {
        //            e.printStackTrace();
        //        }
        //        return result;
        return null;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        //        CommandResult result = null;
        //        try
        //        {
        //            result = host.execute( Commands.getUninstallClientsCommand() );
        //            if ( result.hasSucceeded() )
        //            {
        //                config.getClients().remove( host.getId() );
        //                manager.getPluginDao().saveInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName(),
        // config );
        //                trackerOperation.addLogDone(
        //                        OozieClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
        //                                + " successfully." );
        //            }
        //            else
        //            {
        //                trackerOperation.addLogFailed(
        //                        "Could not uninstall " + OozieClusterConfig.PRODUCT_KEY + " from node " + host
        // .getHostname() );
        //            }
        //        }
        //        catch ( CommandException e )
        //        {
        //            e.printStackTrace();
        //        }
        //        return result;
        return null;
    }


    public static void logStatusResults( TrackerOperation po, CommandResult result )
    {
        Preconditions.checkNotNull( result );
        StringBuilder log = new StringBuilder();
        String status;
        String cmdResult = result.getStdErr() + result.getStdOut();
        if ( cmdResult.contains( "Bootstrap" ) )
        {
            status = "Oozie Server is running";
        }
        else
        {
            status = "Oozie Server is not running";
        }
        log.append( String.format( "%s", status ) );
        po.addLogDone( log.toString() );
    }
}


