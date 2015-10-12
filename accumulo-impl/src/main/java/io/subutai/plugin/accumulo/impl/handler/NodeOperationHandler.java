package io.subutai.plugin.accumulo.impl.handler;


import java.util.List;
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
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.ClusterConfiguration;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.Zookeeper;


/**
 * This class handles operations that are related to just one node.
 *
 * TODO: add nodes and delete node operation should be implemented.
 */
public class NodeOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( NodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NodeType nodeType;
    private Hadoop hadoop;
    private Zookeeper zookeeper;
    private Environment environment;
    private CommandUtil commandUtil;


    public NodeOperationHandler( final AccumuloImpl manager, final Hadoop hadoop, final Zookeeper zookeeper,
                                 final String clusterName, final String hostname, NodeOperationType operationType,
                                 NodeType nodeType )
    {
        super( manager, manager.getCluster( clusterName ) );
        Preconditions.checkNotNull( manager, "Accumulo manager is null." );
        Preconditions.checkNotNull( hadoop, "Hadoop manager is null." );
        Preconditions.checkNotNull( zookeeper, "Zookeeper manager is null." );
        Preconditions.checkNotNull( clusterName, "Accumulo clusterName is null." );
        Preconditions.checkNotNull( hostname, "Hostname is null." );
        Preconditions.checkNotNull( operationType, "Node operation type is null." );

        try
        {
            this.environment = manager.getEnvironmentManager()
                                      .loadEnvironment( manager.getCluster( clusterName ).getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Could not find environment", e );
            e.printStackTrace();
        }
        this.commandUtil = new CommandUtil();
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.hadoop = hadoop;
        this.zookeeper = zookeeper;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public NodeOperationHandler( final AccumuloImpl manager, final Hadoop hadoop, final Zookeeper zookeeper,
                                 final String clusterName, final NodeOperationType nodeOperationType,
                                 final NodeType nodeType )
    {
        super( manager, manager.getCluster( clusterName ) );
        Preconditions.checkNotNull( manager, "Accumulo manager is null." );
        Preconditions.checkNotNull( hadoop, "Hadoop manager is null." );
        Preconditions.checkNotNull( zookeeper, "Zookeeper manager is null." );
        Preconditions.checkNotNull( clusterName, "Accumulo clusterName is null." );
        Preconditions.checkNotNull( nodeOperationType, "Node operation type is null." );
        Preconditions.checkNotNull( nodeType, "Node type is null." );

        try
        {
            this.environment = manager.getEnvironmentManager()
                                      .loadEnvironment( manager.getCluster( clusterName ).getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Could not find environment", e );
            e.printStackTrace();
        }
        this.commandUtil = new CommandUtil();
        this.clusterName = clusterName;
        this.hadoop = hadoop;
        this.zookeeper = zookeeper;
        this.operationType = nodeOperationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Adding new node to accumulo cluster: %s", clusterName ) );
    }


    @Override
    public void run()
    {
        AccumuloClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        EnvironmentContainerHost host;
        try
        {
            host = environment.getContainerHostByHostname( hostname );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with name {%s} doesn't exists in environment: %s", hostname,
                    environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return;
        }

        try
        {
            CommandResult result = null;
            switch ( operationType )
            {
                case START:
                    result = host.execute( Commands.startCommand );
                    break;
                case STOP:
                    result = host.execute( Commands.stopCommand );
                    break;
                case STATUS:
                    result = host.execute( Commands.statusCommand );
                    break;
                case DESTROY:
                    try
                    {
                        removeNode( host );
                    }
                    catch ( ClusterException e )
                    {
                        e.printStackTrace();
                    }
                    break;
                case ADD:
                    addNode();
                    break;
            }
            if ( result != null )
            {
                trackerOperation.addLogDone( result.getStdOut() );
            }
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
            LOGGER.error( String.format( "Command failed" ), e );
        }
    }


    private void removeNode( EnvironmentContainerHost node ) throws ClusterException
    {
        /**
         * 1) sanity checks
         * 2) find role node and determine if it is just regionserver or not
         *    case 1.1: if just has one node role, then remove Accumulo debian package from that node.
         *    case 1.2: else more than one node role, just remove node role from configuration files.
         * 3) Update configuration entry in database
         */

        //check if node is in the cluster
        if ( !config.getAllNodes().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to this cluster", hostname ) );
        }

        List<NodeType> roles = config.getNodeRoles( node.getId() );
        if ( roles.size() == 1 )
        {
            // case 1.1
            // uninstall accumulo from that node
            trackerOperation.addLog( "Removing Accumulo from node..." );
            CommandResult result = executeCommand( node, new RequestBuilder( Commands.uninstallCommand ) );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( "Accumulo is removed successfully" );
            }

            trackerOperation.addLog( "Notifying other nodes in cluster..." );
            // configure other nodes in cluster
            if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) )
            {
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearTracerCommand( node.getHostname() ) ), environment );
                config.getTracers().remove( node.getId() );
            }
            else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) )
            {
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearSlaveCommand( node.getHostname() ) ), environment );
                config.getSlaves().remove( node.getId() );
            }
            stopAccumuloNodeRole( node, nodeType );
            trackerOperation.addLog( "Updating cluster information.." );
            // update configuration
            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated successfully" );
        }
        else
        {
            // case 1.2
            // configure other nodes in cluster
            trackerOperation.addLog( "Notifying other nodes in cluster..." );
            // configure other nodes in cluster
            if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) )
            {
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearTracerCommand( node.getHostname() ) ), environment );
                config.getTracers().remove( node.getId() );
            }
            else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) )
            {
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearSlaveCommand( node.getHostname() ) ), environment );
                config.getSlaves().remove( node.getId() );
            }
            stopAccumuloNodeRole( node, nodeType );
            manager.saveConfig( config );
            trackerOperation.addLogDone( "Cluster information is updated successfully" );
        }
    }


    private void stopAccumuloNodeRole( EnvironmentContainerHost host, NodeType nodeType )
    {
        switch ( nodeType )
        {
            case ACCUMULO_TABLET_SERVER:
                String command = "pkill -f tserver";
                try
                {
                    host.execute( new RequestBuilder( command ) );
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
                break;
            case ACCUMULO_TRACER:
                command = "pkill -f tracer";
                try
                {
                    host.execute( new RequestBuilder( command ) );
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
                break;
        }
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
                throw new ClusterException( String.format( "All nodes in %s cluster are used in Accumulo cluster.",
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
            LOGGER.error( "Could not find container host with given hostname : " + hostname );
            e.printStackTrace();
        }

        assert node != null;
        if ( node.getId() != null )
        {
            try
            {
                /**
                 * if there is already Accumulo debian package installed on container, then
                 * just add the new role to that container, otherwise both install and configure node.
                 */
                CommandResult result = executeCommand( node, new RequestBuilder( Commands.checkIfInstalled ) );
                if ( !result.getStdOut().contains( AccumuloClusterConfig.PRODUCT_PACKAGE ) )
                {
                    // install accumulo to this node
                    executeCommand( node, Commands.getInstallCommand() );
                    CommandResult commandResult =
                            executeCommand( node, new RequestBuilder( Commands.checkIfInstalled ) );
                    if ( !commandResult.getStdOut().contains( AccumuloClusterConfig.PRODUCT_PACKAGE ) )
                    {
                        LOGGER.error( "Accumulo package cannot be installed on container." );
                        trackerOperation.addLogFailed(
                                String.format( "Failed to install Accumulo to %s", node.getHostname() ) );
                        throw new ClusterException( "Accumulo package cannot be installed on container." );
                    }
                }

                clearConfigurationFiles( node );
                if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) )
                {
                    configureOldNodes( node, NodeType.ACCUMULO_TRACER, config );
                    config.getTracers().add( node.getId() );
                    configureNewNode( node, config );
                }
                else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) )
                {
                    clearConfigurationFiles( node );
                    configureOldNodes( node, NodeType.ACCUMULO_TABLET_SERVER, config );
                    config.getSlaves().add( node.getId() );
                    configureNewNode( node, config );
                }

                trackerOperation.addLog( "Saving cluster information..." );
                manager.saveConfig( config );
                trackerOperation.addLog( "Notifying other nodes..." );

                // check if Master is running, then start new region server.
                startNewNode();
                trackerOperation.addLogDone( "New node is added successfully" );
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
                LOGGER.error( "Error while subscribing to alert !", e );
                e.printStackTrace();
            }
        }
    }


    private void startNewNode() throws ClusterException
    {
        try
        {
            EnvironmentContainerHost master = environment.getContainerHostById( config.getMasterNode() );
            CommandResult result = executeCommand( master, Commands.statusCommand );
            if ( result.hasSucceeded() )
            {
                String output[] = result.getStdOut().split( "\n" );
                for ( String part : output )
                {
                    if ( part.toLowerCase().contains( "master" ) )
                    {
                        if ( part.contains( "pid" ) )
                        {
                            executeCommand( master, Commands.stopCommand );
                            executeCommand( master, Commands.startCommand );
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


    private void clearConfigurationFiles( EnvironmentContainerHost host )
    {
        try
        {
            executeCommand( host, Commands.getClearMastersFileCommand( "masters" ) );
            executeCommand( host, Commands.getClearSlavesFileCommand( "slaves" ) );
            executeCommand( host, Commands.getClearMastersFileCommand( "tracers" ) );
            executeCommand( host, Commands.getClearMastersFileCommand( "gc" ) );
            executeCommand( host, Commands.getClearMastersFileCommand( "monitor" ) );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }


    private void configureOldNodes( EnvironmentContainerHost host, NodeType nodeType, AccumuloClusterConfig config )
    {


        if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) )
        {
            Set<String> allNodes = config.getAllNodes();
            allNodes.remove( host.getId() );
            ClusterConfiguration
                    .executeCommandOnAllContainer( allNodes, Commands.getAddTracersCommand( host.getHostname() ),
                            environment );
        }
        else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) )
        {
            Set<String> allNodes = config.getAllNodes();
            allNodes.remove( host.getId() );
            ClusterConfiguration
                    .executeCommandOnAllContainer( allNodes, Commands.getAddSlavesCommand( host.getHostname() ),
                            environment );
        }
    }


    private void configureNewNode( EnvironmentContainerHost host, AccumuloClusterConfig config )
    {
        try
        {
            executeCommand( host,
                    Commands.getAddMasterCommand( serializeHostName( Sets.newHashSet( config.getMasterNode() ) ) ) );
            executeCommand( host,
                    Commands.getAddMonitorCommand( serializeHostName( Sets.newHashSet( config.getMonitor() ) ) ) );
            executeCommand( host,
                    Commands.getAddGCCommand( serializeHostName( Sets.newHashSet( config.getGcNode() ) ) ) );
            executeCommand( host, Commands.getAddTracersCommand( serializeHostName( config.getTracers() ) ) );
            executeCommand( host, Commands.getAddSlavesCommand( serializeHostName( config.getSlaves() ) ) );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }


    private String serializeHostName( Set<String> ids )
    {
        StringBuilder slavesSpaceSeparated = new StringBuilder();
        for ( String id : ids )
        {
            slavesSpaceSeparated.append( getHost( environment, id ).getHostname() ).append( " " );
        }
        return slavesSpaceSeparated.toString();
    }


    private EnvironmentContainerHost getHost( Environment environment, String nodeId )
    {
        try
        {
            return environment.getContainerHostById( nodeId );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container host with id: %s doesn't exists in environment: %s", nodeId,
                    environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
    }
}
