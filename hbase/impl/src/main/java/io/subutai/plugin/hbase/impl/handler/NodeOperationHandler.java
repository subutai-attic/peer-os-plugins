package io.subutai.plugin.hbase.impl.handler;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.impl.ClusterConfiguration;
import io.subutai.plugin.hbase.impl.Commands;
import io.subutai.plugin.hbase.impl.HBaseImpl;


public class NodeOperationHandler extends AbstractOperationHandler<HBaseImpl, HBaseConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    private String hostname;
    private NodeOperationType operationType;
    private NodeType nodeType;
    private HBaseConfig config;
    private EnvironmentContainerHost node;
    private Environment environment;


    public NodeOperationHandler( final HBaseImpl manager, final HBaseConfig config, final String hostname,
                                 final NodeOperationType operationType, final NodeType nodeType )
    {
        super( manager, config );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Invalid hostname" );
        Preconditions.checkNotNull( operationType );
        this.hostname = hostname;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.config = config;
        try
        {
            this.environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            this.node = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Environment not found", e );
            trackerOperation.addLogFailed( "Environment not found" );
        }
        trackerOperation = manager.getTracker().createTrackerOperation( HBaseConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
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
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }
        runCommand( host, operationType );
    }


    protected void runCommand( EnvironmentContainerHost host, NodeOperationType operationType )
    {
        switch ( operationType )
        {
            case START:
                startNode();
                break;
            case STOP:
                stopNode();
                break;
            case STATUS:
                checkServiceStatus( host );
                break;
            case ADD:
                addNode();
                break;
            case DESTROY:
                try
                {
                    removeNode();
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Could not remove region server !", e );
                    e.printStackTrace();
                }
                break;
        }
    }


    private void stopNode()
    {
        try
        {
            CommandResult result = node.execute( nodeType == NodeType.MASTER_NODE ? Commands.getStopCommand() :
                                                 Commands.getStopRegionServerCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }

            node.execute( new RequestBuilder( "sleep 10" ) );

            trackerOperation.addLogDone( "Stop Regionserver command executed" );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private void startNode()
    {
        try
        {
            CommandResult result = node.execute( nodeType == NodeType.MASTER_NODE ? Commands.getStartCommand() :
                                                 Commands.getStartRegionServerCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }
            trackerOperation.addLogDone( "Start Regionserver command executed" );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private void addNode()
    {
        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( config.getHadoopClusterName() );

        List<String> hadoopNodes = hadoopClusterConfig.getAllNodes();
        hadoopNodes.removeAll( config.getAllNodes() );

        if ( hadoopNodes.isEmpty() )
        {
            try
            {
                throw new ClusterException( String.format( "All nodes in %s cluster are used in HBase cluster.",
                        config.getHadoopClusterName() ) );
            }
            catch ( ClusterException e )
            {
                e.printStackTrace();
            }
        }

        EnvironmentContainerHost node = null;
        try
        {
            node = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Could not find container host with given hostname : " + hostname );
            e.printStackTrace();
        }

        try
        {
            // execute apt-get update
            executeCommand( node, new RequestBuilder( Commands.UPDATE_COMAND ).withTimeout( 20000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO ) );

            CommandResult result = node.execute( Commands.getCheckInstalledCommand() );
            if ( !result.getStdOut().contains( Commands.PACKAGE_NAME ) )
            {
                // install hbase to this node
                executeCommand( node, Commands.getInstallCommand() );
                CommandResult commandResult = node.execute( Commands.getCheckInstalledCommand() );
                if ( !commandResult.getStdOut().contains( HBaseConfig.PRODUCT_NAME.toLowerCase() ) )
                {
                    LOG.error( "HBase package cannot be installed on container." );
                    trackerOperation
                            .addLogFailed( String.format( "Failed to install HBase to %s", node.getHostname() ) );
                    throw new ClusterException( "HBase package cannot be installed on container." );
                }
            }

            ClusterConfiguration clusterConfiguration =
                    new ClusterConfiguration( trackerOperation, manager, manager.getHadoopManager() );
            clusterConfiguration.addnode( config, node, environment );
            config.getRegionServers().add( node.getId() );

            trackerOperation.addLog( "Saving cluster information..." );
            manager.saveConfig( config );
            trackerOperation.addLog( "Notifying other nodes..." );
            trackerOperation.addLogDone( "New node is added successfully" );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }

        try
        {
            manager.subscribeToAlerts( node );
        }
        catch ( MonitorException e )
        {
            LOG.error( "Error while subscribing to alert !", e );
            e.printStackTrace();
        }
    }


    private void restartCluster() throws ClusterException
    {
        try
        {
            EnvironmentContainerHost hmaster = environment.getContainerHostById( config.getHbaseMaster() );
            CommandResult result = executeCommand( hmaster, Commands.getStatusCommand() );
            if ( result.hasSucceeded() )
            {
                String output[] = result.getStdOut().split( "\n" );
                for ( String part : output )
                {
                    if ( part.toLowerCase().contains( NodeType.HMASTER.name().toLowerCase() ) )
                    {
                        if ( part.contains( "pid" ) )
                        {
                            executeCommand( hmaster, Commands.getStopCommand() );
                            executeCommand( hmaster, Commands.getStartCommand() );
                        }
                    }
                }
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    private void checkServiceStatus( EnvironmentContainerHost host )
    {
        try
        {
            boolean isLogged = false;

            List<NodeType> roles = config.getNodeRoles( host );

            CommandResult result = node.execute( Commands.getStatusCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }
            else
            {
                trackerOperation.addLogFailed( result.getStdErr() );
            }
            trackerOperation.addLogDone( "Check service status command executed" );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( e.getMessage() );
            LOG.error( e.getMessage(), e );
        }
    }


    private void removeNode() throws ClusterException
    {
        //check if node is in the cluster
        if ( !config.getAllNodes().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to this cluster", hostname ) );
        }

        trackerOperation.addLog( "Removing HBase from node..." );

        ClusterConfiguration clusterConfiguration =
                new ClusterConfiguration( trackerOperation, manager, manager.getHadoopManager() );
        clusterConfiguration.clearConfigurationFiles( config, node, environment );

        CommandResult result = executeCommand( node, Commands.getUninstallCommand() );

        if ( result.hasSucceeded() )
        {
            trackerOperation.addLog( "HBase is removed successfully" );
        }

        trackerOperation.addLog( "Updating cluster information.." );
        config.getRegionServers().remove( node.getId() );
        manager.saveConfig( config );
        restartCluster();
        trackerOperation.addLogDone( "Cluster information is updated successfully" );
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command, boolean skipError )
            throws ClusterException
    {
        CommandResult result = null;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            if ( skipError )
            {
                trackerOperation
                        .addLog( String.format( "Error on container %s: %s", host.getHostname(), e.getMessage() ) );
            }
            else
            {
                throw new ClusterException( e );
            }
        }
        if ( skipError )
        {
            if ( result != null && !result.hasSucceeded() )
            {
                trackerOperation.addLog( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        else
        {
            if ( !result.hasSucceeded() )
            {
                throw new ClusterException( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        return result;
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command ) throws ClusterException
    {
        return executeCommand( host, command, false );
    }
}
