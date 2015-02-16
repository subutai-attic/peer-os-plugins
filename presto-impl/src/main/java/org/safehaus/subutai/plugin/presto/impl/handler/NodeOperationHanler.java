package org.safehaus.subutai.plugin.presto.impl.handler;


import java.util.Iterator;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;
import org.safehaus.subutai.plugin.presto.impl.Commands;
import org.safehaus.subutai.plugin.presto.impl.PrestoImpl;
import org.safehaus.subutai.plugin.presto.impl.SetupHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;


public class NodeOperationHanler extends AbstractOperationHandler<PrestoImpl, PrestoClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHanler.class);
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
        ContainerHost coordinator = null;
        try
        {
            coordinator = environment.getContainerHostById( config.getCoordinatorNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
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


    private CommandResult installProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            if ( ! host.isConnected() )
            {
                throw new ClusterSetupException( "New node is not connected" );
            }
            if ( config.getWorkers().contains( host.getId() ) )
            {
                throw new ClusterSetupException( "Node already belongs to cluster" + clusterName );
            }
            result = host.execute( manager.getCommands().getCheckInstalledCommand() );
            String hadoopPackage = Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME;
            boolean skipInstall = false;
            if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
            {
                skipInstall = true;
                trackerOperation.addLog( "Node already has Presto installed" );
            }
            else if ( ! result.getStdOut().contains( hadoopPackage ) )
            {
                throw new ClusterSetupException( "Node has no Hadoop installation" );
            }

            if ( ! skipInstall )
            {
                trackerOperation.addLog( "Installing Presto..." );
                result = host.execute( manager.getCommands().getInstallCommand() );
                if ( result.hasSucceeded() )
                {
                    config.getWorkers().add( host.getId() );
                    manager.saveConfig( config );
                    trackerOperation.addLog(
                            PrestoClusterConfig.PRODUCT_KEY + " is installed on node " + host.getHostname()
                                    + " successfully." );
                }
                else
                {
                    trackerOperation.addLogFailed(
                            "Could not install " + PrestoClusterConfig.PRODUCT_KEY + " to node " + host.getHostname() );
                }
            }

            Set<ContainerHost> set = Sets.newHashSet( host );
            SetupHelper sh = new SetupHelper( trackerOperation, manager, config );
            sh.configureAsWorker( set );

            // check if coordinator is already running,
            // then newly added node should be started automatically.
            Environment environment = null;
            try
            {
                environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
                try
                {
                    ContainerHost coordinator = environment.getContainerHostById( config.getCoordinatorNode() );
                    RequestBuilder checkMasterIsRunning = manager.getCommands().getStatusCommand();
                    result = commandUtil.execute( checkMasterIsRunning, coordinator );
                    if ( result.hasSucceeded() ){
                        // if Presto service is running on container,
                        // command output will be smt like this: "Running as {pid}"
                        // else it returns "Not running"
                        if ( ! result.getStdOut().toLowerCase().contains( "not running" ) ){
                            sh.startNodes( set );
                        }
                    }
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Error getting environment by id: " + config.getEnvironmentId().toString(), e );
            }


            //subscribe to alerts
            try
            {
                manager.subscribeToAlerts( host );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }

            trackerOperation.addLogDone( "Node configured" );
        }
        catch ( CommandException | ClusterSetupException | ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
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
        log.append( "UNKNOWN" );
        if ( result.getExitCode() == 0 )
        {
            log.append( result.getStdOut() );
        }
        if ( result.getExitCode() == 768 )
        {
            log.append( "Not running" );
        }
        po.addLogDone( log.toString() );
    }
}
