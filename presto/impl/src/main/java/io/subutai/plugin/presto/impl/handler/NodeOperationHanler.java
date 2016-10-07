package io.subutai.plugin.presto.impl.handler;


import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import io.subutai.plugin.presto.impl.Commands;
import io.subutai.plugin.presto.impl.PrestoImpl;
import io.subutai.plugin.presto.impl.SetupHelper;


public class NodeOperationHanler extends AbstractOperationHandler<PrestoImpl, PrestoClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHanler.class );
    private String clusterName;
    private String hostName;
    private NodeOperationType operationType;
    CommandUtil commandUtil;


    public NodeOperationHanler( final PrestoImpl manager, final String clusterName, final String hostName,
                                NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostName = hostName;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( PrestoClusterConfig.PRODUCT_KEY,
                String.format( "Checking %s cluster...", clusterName ) );
        commandUtil = new CommandUtil();
    }


    @Override
    public void run()
    {
        PrestoClusterConfig config = manager.getCluster( clusterName );
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
        Iterator iterator = environment.getContainerHosts().iterator();
        EnvironmentContainerHost host = null;
        while ( iterator.hasNext() )
        {
            host = ( EnvironmentContainerHost ) iterator.next();
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
        EnvironmentContainerHost coordinator;
        try
        {
            coordinator = environment.getContainerHostById( config.getCoordinatorNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
            return;
        }
        if ( !coordinator.isConnected() )
        {
            trackerOperation
                    .addLogFailed( String.format( "Coordinator node %s is not connected", coordinator.getHostname() ) );
            return;
        }

        try
        {
            CommandResult result;
            switch ( operationType )
            {
                case START:
                    result = host.execute( manager.getCommands().getStartCommand().daemon() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STOP:
                    result = host.execute( manager.getCommands().getStopCommand() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STATUS:
                    result = host.execute( manager.getCommands().getStatusCommand() );
                    logStatusResults( trackerOperation, result );
                    break;
                case INSTALL:
                    installProductOnNode( host );
                    break;
                case UNINSTALL:
                    uninstallProductOnNode( host );
                    break;
            }
            //logStatusResults( trackerOperation, result );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    private CommandResult installProductOnNode( EnvironmentContainerHost host )
    {
        CommandResult result = null;
        try
        {
            if ( !host.isConnected() )
            {
                throw new ClusterSetupException( "New node is not connected" );
            }
            if ( config.getWorkers().contains( host.getId() ) )
            {
                throw new ClusterSetupException( "Node already belongs to cluster" + clusterName );
            }
            result = host.execute( Commands.getCheckInstalledCommand() );

            trackerOperation.addLog( "Installing Presto..." );
            result = host.execute( manager.getCommands().getInstallCommand() );
            CommandResult checkResult = host.execute( Commands.getCheckInstalledCommand() );
            if ( result.hasSucceeded() && checkResult.getStdOut().contains( Commands.PACKAGE_NAME ) )
            {
                config.getWorkers().add( host.getId() );
                manager.saveConfig( config );
                trackerOperation.addLog( PrestoClusterConfig.PRODUCT_KEY + " is installed on node " + host.getHostname()
                        + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not install " + PrestoClusterConfig.PRODUCT_KEY + " to node " + host.getHostname() );
            }

            Set<EnvironmentContainerHost> set = Sets.newHashSet( host );
            SetupHelper sh = new SetupHelper( trackerOperation, manager, config );
            sh.configureAsWorker( set, manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                              .getContainerHostById( config.getCoordinatorNode() ) );
            host.execute( manager.getCommands().getStartCommand() );


            //subscribe to alerts
            /*try
            {
                manager.subscribeToAlerts( host );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }*/

            trackerOperation.addLogDone( "Node configured" );
        }
        catch ( CommandException | ClusterSetupException | ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( EnvironmentContainerHost host )
    {
        CommandResult result = null;
        try
        {
            host.execute( manager.getCommands().getStopCommand() );
            result = host.execute( manager.getCommands().getUninstallCommand() );
            if ( result.hasSucceeded() )
            {
                config.getWorkers().remove( host.getId() );
                manager.saveConfig( config );
                trackerOperation.addLogDone(
                        PrestoClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not uninstall " + PrestoClusterConfig.PRODUCT_KEY + " from node " + host.getHostname() );
            }
        }
        catch ( CommandException | ClusterException e )
        {
            e.printStackTrace();
        }
        return result;
    }


    public static void logStatusResults( TrackerOperation po, CommandResult result )
    {
        Preconditions.checkNotNull( result );
        StringBuilder log = new StringBuilder();
        if ( result.getStdOut().contains( "PrestoServer" ) )
        {
            log.append( "Running as" );
        }
        else
        {
            log.append( "Not running" );
        }
        po.addLogDone( log.toString() );
    }
}
