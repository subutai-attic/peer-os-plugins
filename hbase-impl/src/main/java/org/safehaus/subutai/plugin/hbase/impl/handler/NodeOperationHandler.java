package org.safehaus.subutai.plugin.hbase.impl.handler;


import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.safehaus.subutai.plugin.hbase.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.hbase.impl.Commands;
import org.safehaus.subutai.plugin.hbase.impl.HBaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class NodeOperationHandler extends AbstractOperationHandler<HBaseImpl, HBaseConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    private String hostname;
    private NodeOperationType operationType;
    private HBaseConfig config;
    private ContainerHost node;
    private Environment environment;


    public NodeOperationHandler( final HBaseImpl manager, final HBaseConfig config, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, config );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Invalid hostname" );
        Preconditions.checkNotNull( operationType );
        this.hostname = hostname;
        this.operationType = operationType;
        this.config = config;
        try
        {
            this.environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
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
        runCommand( host, operationType );
    }


    protected void runCommand( ContainerHost host, NodeOperationType operationType )
    {
        switch ( operationType )
        {
            case START:
                break;
            case STOP:
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


    private void addNode()
    {
        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( config.getHadoopClusterName() );

        List<UUID> hadoopNodes = hadoopClusterConfig.getAllNodes();
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

        assert node != null;
        if ( node.getId() != null )
        {
            try
            {
                // install hbase to this node
                CommandResult result = executeCommand( node, Commands.getInstallCommand() );
                if ( result.hasSucceeded() )
                {
                    // configure new node
                    config.getRegionServers().add( node.getId() );
                    trackerOperation.addLog( "Saving cluster information..." );
                    manager.saveConfig( config );

                    configureExistingNodes( node );
                    try
                    {
                        new ClusterConfiguration( trackerOperation, manager, manager.getHadoopManager() )
                                .configureNewRegionServerNode( config, environment, node );
                    }
                    catch ( ClusterConfigurationException e )
                    {
                        LOG.error( "Error while configuration !", e );
                        e.printStackTrace();
                    }


                    // check if HMaster is running, then start new region server.
                    startNewNode( node );
                    trackerOperation.addLogDone( "Region server is added succesfully" );
                }
                else
                {
                    // could not install hbase to this node
                    trackerOperation
                            .addLogFailed( String.format( "Failed to install HBase to %s", node.getHostname() ) );
                }
            }
            catch ( ClusterException e )
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
    }


    private void startNewNode( ContainerHost host ) throws ClusterException
    {
        try
        {
            ContainerHost hmaster = environment.getContainerHostById( config.getHbaseMaster() );
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
                            executeCommand( host, Commands.getStartRegionServer() );
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


    private void configureExistingNodes( ContainerHost host )
    {
        ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                Commands.getConfigRegionCommand( host.getHostname() ), environment );
    }


    private void checkServiceStatus( ContainerHost host )
    {
        try
        {
            boolean isLogged = false;

            List<NodeType> roles = config.getNodeRoles( host );

            CommandResult result = node.execute( Commands.getStatusCommand() );
            if ( result.hasSucceeded() )
            {
                String output[] = result.getStdOut().split( "\n" );
                for ( String part : output )
                {
                    for ( NodeType role : roles )
                    {

                        if ( role.equals( NodeType.BACKUPMASTER ) )
                        {
                            role = NodeType.HMASTER;
                        }
                        if ( part.toLowerCase().contains( role.name().toLowerCase() ) )
                        {
                            trackerOperation.addLog( part );
                            isLogged = true;
                            break;
                        }
                    }
                }
                if ( !isLogged )
                {
                    trackerOperation.addLog( result.getStdOut() );
                }
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
        /**
         * 1) sanity checks
         * 2) find role node and determine if it is just regionserver or not
         *    case 1.1: if just regionserver, then remove HBase debian package from that node.
         *    case 1.2: else just remove regionserver entry from configuration files and stop regionserver service
         * 3) Update configuration entry in database
         */

        //check if node is in the cluster
        if ( !config.getAllNodes().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to this cluster", hostname ) );
        }

        List<NodeType> roles = config.getNodeRoles( node );
        if ( roles.size() == 1 )
        {
            // case 1.1
            // uninstall hbase from that node
            trackerOperation.addLog( "Removing HBase from node..." );
            CommandResult result = executeCommand( node, Commands.getUninstallCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( "HBase is removed successfully" );
            }

            // configure other nodes in cluster
            trackerOperation.addLog( "Notifying other nodes in cluster..." );
            ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                    Commands.getRemoveRegionServerCommand( node.getHostname() ), environment );

            trackerOperation.addLog( "Updating cluster information.." );
            // update configuration
            config.getRegionServers().remove( node.getId() );
            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated successfully" );
        }
        else
        {
            // case 1.2
            // configure other nodes in cluster
            trackerOperation.addLog( "Notifying other nodes in cluster..." );
            ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                    Commands.getRemoveRegionServerCommand( node.getHostname() ), environment );

            executeCommand( node, Commands.getStopRegionServer() );

            trackerOperation.addLog( "Updating cluster information.." );
            // update configuration
            config.getRegionServers().remove( node.getId() );
            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated successfully" );
        }
    }


    public CommandResult executeCommand( ContainerHost host, RequestBuilder command, boolean skipError )
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


    public CommandResult executeCommand( ContainerHost host, RequestBuilder command ) throws ClusterException
    {
        return executeCommand( host, command, false );
    }
}
