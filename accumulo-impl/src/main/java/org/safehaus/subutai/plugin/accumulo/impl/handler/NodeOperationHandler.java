package org.safehaus.subutai.plugin.accumulo.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.accumulo.impl.AccumuloImpl;
import org.safehaus.subutai.plugin.accumulo.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.accumulo.impl.Commands;
import org.safehaus.subutai.plugin.accumulo.impl.Util;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


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
                                      .findEnvironment( manager.getCluster( clusterName ).getEnvironmentId() );
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
                                      .findEnvironment( manager.getCluster( clusterName ).getEnvironmentId() );
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

        ContainerHost host;
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


    private void removeNode( ContainerHost node ) throws ClusterException
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
            if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) ){
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearTracerCommand( node.getHostname() ) ), environment );
                config.getTracers().remove( node.getId() );
            }
            else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) ){
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(), new RequestBuilder( Commands
                        .getClearSlaveCommand( node.getHostname() ) ), environment );
                config.getSlaves().remove( node.getId() );
            }
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
            if ( nodeType.equals( NodeType.ACCUMULO_TRACER ) ){
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(),
                        new RequestBuilder( Commands.getClearTracerCommand( node.getHostname() ) ), environment );
                config.getTracers().remove( node.getId() );
            }
            else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) ){
                ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(), new RequestBuilder( Commands
                        .getClearSlaveCommand( node.getHostname() ) ), environment );
                config.getSlaves().remove( node.getId() );
            }
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


//    /**
//     * Adds specified container to existing cluster. Checks if container already configured in cluster, if not, adds
//     * environment container first to hadoop cluster and zookeeper cluster, finally installs accumulo and triggers
//     * cluster configuration with new environment container.
//     *
//     * @param clusterName - user specified clusterName
//     * @param containerName - user specified environment container name
//     * @param nodeType - new node role
//     *
//     * @return - trackerOperationViewId
//     */
//    private CommandResult addNode( final String clusterName, final String containerName, final NodeType nodeType )
//    {
//        CommandResult result;
//
//        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
//        ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( config.getZookeeperClusterName() );
//
//        ContainerHost environmentContainerHost;
//        try
//        {
//            environmentContainerHost = environment.getContainerHostByHostname( containerName );
//        }
//        catch ( ContainerHostNotFoundException e )
//        {
//            String msg = String.format( "Container with name: %s doesn't exists in environment: %s", containerName,
//                    environment.getName() );
//            trackerOperation.addLogFailed( msg );
//            LOGGER.error( msg, e );
//            return null;
//        }
//
//        //check if passed container is valid container
//        if ( environmentContainerHost == null )
//        {
//            trackerOperation.addLogFailed(
//                    String.format( "There is no environment contain with containerName: %s", containerName ) );
//            return null;
//        }
//
//        //check if container already in accumulo cluster
//        if ( config.getAllNodes().contains( environmentContainerHost.getId() ) )
//        {
//            trackerOperation.addLogFailed(
//                    String.format( "Environment container: %s already configured in accumulo cluster.",
//                            containerName ) );
//        }
//
//        //check if container already running hadoop if not install it
//        if ( !hadoopClusterConfig.getAllNodes().contains( environmentContainerHost.getId() ) )
//        {
//            trackerOperation
//                    .addLog( String.format( "Container: %s isn't configured in hadoop cluster.", containerName ) );
//            trackerOperation.addLog( String.format( "Adding container: %s to hadoop cluster: %s", containerName,
//                    hadoopClusterConfig.getClusterName() ) );
//            hadoop.addNode( hadoopClusterConfig.getClusterName(), containerName );
//        }
//
//        //check if container already running zookeeper if not install it
//        if ( !zookeeperClusterConfig.getNodes().contains( environmentContainerHost.getId() ) )
//        {
//            trackerOperation
//                    .addLog( String.format( "Container: %s isn't configured in zookeeper cluster.", containerName ) );
//            trackerOperation.addLog( String.format( "Adding container %s to zookeeper cluster: %s", containerName,
//                    zookeeperClusterConfig.getClusterName() ) );
//            zookeeper.addNode( zookeeperClusterConfig.getClusterName(), containerName );
//        }
//
//
//        //installing accumulo package
//        //initializing accumulo cluster configuration with new node
//        result = installProductOnNode( environmentContainerHost, nodeType );
//        trackerOperation.addLogDone( "" );
//        return result;
//    }



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

        ContainerHost node = null;
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
                 * if there is already HBase debian package installed on container, then
                 * just add the new role to that container, otherwise both install and configure node.
                 */
                CommandResult result = executeCommand( node, new RequestBuilder( Commands.checkIfInstalled  ) );
                if ( ! result.getStdOut().contains( AccumuloClusterConfig.PRODUCT_PACKAGE ) ){
                    // install hbase to this node
                    executeCommand( node, Commands.getInstallCommand() );
                    CommandResult commandResult = executeCommand( node, new RequestBuilder( Commands.checkIfInstalled  ) );
                    if ( ! commandResult.getStdOut().contains( AccumuloClusterConfig.PRODUCT_PACKAGE ) ){
                        LOGGER.error( "Accumulo package cannot be installed on container." );
                        trackerOperation
                                .addLogFailed( String.format( "Failed to install HBase to %s", node.getHostname() ) );
                        throw new ClusterException( "Accumulo package cannot be installed on container." );
                    }
                }

                if ( nodeType.equals( NodeType.ACCUMULO_TRACER  ) ){
                    if ( ! config.getAllNodes().contains( node.getId() ) ) {
                        clearConfigurationFiles( node );
                    }
                    config.getTracers().add( node.getId() );
                    configureNewNode( node, NodeType.ACCUMULO_TRACER, config );
                }
                else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) ){
                    if ( ! config.getAllNodes().contains( node.getId() ) ) {
                        clearConfigurationFiles( node );
                    }
                    config.getSlaves().add( node.getId() );
                    configureNewNode( node, NodeType.ACCUMULO_TABLET_SERVER, config );
                }

                trackerOperation.addLog( "Saving cluster information..." );
                manager.saveConfig( config );
                trackerOperation.addLog( "Notifying other nodes..." );

                // check if HMaster is running, then start new region server.
                startNewNode( node );
                trackerOperation.addLogDone( "New node is added succesfully" );
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


    private void startNewNode( ContainerHost host ) throws ClusterException
    {
        try
        {
            ContainerHost hmaster = environment.getContainerHostById( config.getMasterNode() );
            CommandResult result = executeCommand( hmaster, Commands.statusCommand );
            if ( result.hasSucceeded() )
            {
                String output[] = result.getStdOut().split( "\n" );
                for ( String part : output )
                {
                    if ( part.toLowerCase().contains( NodeType.HMASTER.name().toLowerCase() ) )
                    {
                        if ( part.contains( "pid" ) )
                        {
                            executeCommand( hmaster, Commands.stopCommand );
                            executeCommand( hmaster, Commands.startCommand );
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

    private void clearConfigurationFiles( ContainerHost host ){
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


    private void configureNewNode( ContainerHost host, NodeType nodeType, AccumuloClusterConfig config )
    {
        if ( nodeType.equals( NodeType.ACCUMULO_TRACER  ) ){
            ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(), Commands.getAddTracersCommand(
                    serializeHostName( config.getTracers() ) ), environment );
        }
        else if ( nodeType.equals( NodeType.ACCUMULO_TABLET_SERVER ) ){
            ClusterConfiguration.executeCommandOnAllContainer( config.getAllNodes(), Commands.getAddSlavesCommand(
                    serializeHostName( config.getSlaves() ) ), environment );
        }
    }


    private String serializeHostName( Set<UUID> uuids ){
        StringBuilder slavesSpaceSeparated = new StringBuilder();
        for ( UUID tracer : config.getTracers() )
        {
            slavesSpaceSeparated.append( getHost( environment, tracer ).getHostname() ).append( " " );
        }
        return slavesSpaceSeparated.toString();
    }


    private ContainerHost getHost( Environment environment, UUID nodeId )
    {
        try
        {
            return environment.getContainerHostById( nodeId );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg =
                    String.format( "Container host with id: %s doesn't exists in environment: %s", nodeId.toString(),
                            environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
    }


    /**
     * Add node to specified cluster, checks if there is environment containers with no accumulo installed, if there is,
     * installs hadoop, zookeeper, accumulo on existing environment container otherwise creates environment container
     * with hadoopManager, on top of it installs zookeeper, accumulo finally initializes accumulo cluster configuration
     * procedure
     *
     * @param clusterName - user specified accumulo cluster
     * @param nodeType - new node role
     */
    private CommandResult addNode( final String clusterName, final NodeType nodeType )
    {
        CommandResult result;

        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
        ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( config.getZookeeperClusterName() );

        //get environment containers
        List<ContainerHost> environmentContainers = new ArrayList<>( environment.getContainerHosts() );

        //remove containers which in current accumulo cluster
        Set<UUID> accumuloNodes = config.getAllNodes();
        for ( int i = 0; i < environmentContainers.size(); i++ )
        {
            ContainerHost containerHost = environmentContainers.get( i );
            if ( accumuloNodes.contains( containerHost.getId() ) )
            {
                environmentContainers.remove( i-- );
            }
        }

        //remove hosts which are not configured in zookeeper cluster
        Set<UUID> zookeeperClusterHostIds = zookeeperClusterConfig.getNodes();
        for ( int i = 0; i < environmentContainers.size(); i++ )
        {
            ContainerHost containerHost = environmentContainers.get( i );
            if ( !zookeeperClusterHostIds.contains( containerHost.getId() ) )
            {
                environmentContainers.remove( i-- );
            }
        }

        //remove hosts hosts which are not configured in hadoop cluster
        List<UUID> hadoopClusterHostIds = hadoopClusterConfig.getAllNodes();
        for ( int i = 0; i < environmentContainers.size(); i++ )
        {
            ContainerHost containerHost = environmentContainers.get( i );
            if ( !hadoopClusterHostIds.contains( containerHost.getId() ) )
            {
                environmentContainers.remove( i-- );
            }
        }

        //add new node if all environment containers are already in accumulo cluster
        //otherwise proceed with existing environment container
        ContainerHost additionalNode;
        if ( environmentContainers.isEmpty() )
        {
            //add new node and get its uuid
            zookeeper.addNode( zookeeperClusterConfig.getClusterName() );

            hadoop.addNode( hadoopClusterConfig.getClusterName() );
            HadoopClusterConfig freshHadoopClusterConfig = hadoop.getCluster( hadoopClusterConfig.getClusterName() );
            List<UUID> freshContainerHostIds = freshHadoopClusterConfig.getAllNodes();
            freshContainerHostIds.removeAll( hadoopClusterConfig.getAllNodes() );
            try
            {
                additionalNode = environment.getContainerHostById( freshContainerHostIds.iterator().next() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                String msg = String.format( "Container host with id: %s is not found in environment: %s",
                        freshContainerHostIds.iterator().next().toString(), environment.getName() );
                trackerOperation.addLogFailed( msg );
                LOGGER.error( msg, e );
                return null;
            }
            try
            {
                manager.subscribeToAlerts( additionalNode );
            }
            catch ( MonitorException e )
            {
                LOGGER.error( "Error subscribing container to alerts", e );
            }
        }
        else
        {
            additionalNode = environmentContainers.iterator().next();
        }

        if ( additionalNode == null )
        {
            String msg = "Couldn't create container host.";
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg );
            return null;
        }

        if ( !zookeeperClusterConfig.getNodes().contains( additionalNode.getId() ) )
        {
            trackerOperation.addLog( String.format( "Container: %s isn't configured in zookeeper cluster.",
                    additionalNode.getHostname() ) );
            trackerOperation.addLog(
                    String.format( "Adding container %s to zookeeper cluster: %s", additionalNode.getHostname(),
                            zookeeperClusterConfig.getClusterName() ) );
            zookeeper.addNode( zookeeperClusterConfig.getClusterName(), additionalNode.getHostname() );
        }


        result = installProductOnNode( additionalNode, nodeType );
        trackerOperation.addLogDone( "" );
        return result;
    }


    private CommandResult installProductOnNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;
        try
        {

            result = host.execute( Commands.getInstallCommand().withTimeout( 3600 ) );
            if ( result.hasSucceeded() )
            {
                switch ( nodeType )
                {
                    case ACCUMULO_TRACER:
                        config.getTracers().add( host.getId() );
                        break;
                    case ACCUMULO_TABLET_SERVER:
                        config.getSlaves().add( host.getId() );
                        break;
                }

                // Configure all nodes again
                try
                {
                    manager.subscribeToAlerts( host );
                    Environment environment;
                    try
                    {
                        environment = manager.getEnvironmentManager().findEnvironment(
                                hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        String msg = String.format( "Environment with id: %s doesn't exists.",
                                hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId().toString() );
                        trackerOperation.addLogFailed( msg );
                        LOGGER.error( msg, e );
                        return null;
                    }
                    new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, environment );
                }
                catch ( ClusterConfigurationException | MonitorException e )
                {
                    LOGGER.error( "Error configuring cluster after install product operation on node.", e );
                }

                manager.getPluginDAO().saveInfo( AccumuloClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLog(
                        AccumuloClusterConfig.PRODUCT_KEY + " is installed on node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not install " + AccumuloClusterConfig.PRODUCT_KEY + " to node " + hostname );
            }
        }
        catch ( CommandException e )
        {
            LOGGER.error( "Error installing product on node.", e );
            e.printStackTrace();
        }
        return result;
    }


    /**
     * Completely uninstalls accumulo package from container host if it is not one of master nodes otherwise triggers
     * reconfigure operation on top of cluster for new cluster structure
     *
     * @param host - target container host
     * @param nodeType - node type being removed
     *
     * @return - result of uninstall command
     */
    private CommandResult uninstallProductOnNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;
        switch ( nodeType )
        {
            case ACCUMULO_TRACER:
                if ( config.getTracers().size() <= 1 )
                {
                    trackerOperation.addLogFailed( "Could not uninstall last tracer of cluster." );
                    return null;
                }
                break;
            case ACCUMULO_TABLET_SERVER:
                if ( config.getSlaves().size() <= 1 )
                {
                    trackerOperation.addLogFailed( "Could not uninstall last tablet server of cluster." );
                    return null;
                }
                break;
        }
        try
        {
            UUID targetHostId = host.getId();
            if ( !targetHostId.equals( config.getGcNode() ) &&
                    !targetHostId.equals( config.getMonitor() ) &&
                    !targetHostId.equals( config.getMasterNode() ) )
            {
                result = host.execute( new RequestBuilder( Commands.uninstallCommand ) );
                if ( !result.hasSucceeded() )
                {
                    trackerOperation.addLogFailed(
                            "Could not uninstall " + AccumuloClusterConfig.PRODUCT_KEY + " from node " + hostname );
                    return result;
                }
            }

            Set<Host> hostSet = Util.getHosts( config, environment );
            Map<Host, CommandResult> resultMap = null;
            switch ( nodeType )
            {
                case ACCUMULO_TRACER:
                    config.getTracers().remove( host.getId() );
                    resultMap = commandUtil.executeParallel(
                            new RequestBuilder( Commands.getClearTracerCommand( host.getHostname() ) ), hostSet );

                    break;
                case ACCUMULO_TABLET_SERVER:
                    config.getSlaves().remove( host.getId() );
                    resultMap = commandUtil
                            .executeParallel( new RequestBuilder( Commands.getClearSlaveCommand( host.getHostname() ) ),
                                    hostSet );
                    break;
            }
            if ( resultMap != null )
            {
                if ( Util.isAllSuccessful( resultMap, hostSet ) )
                {
                    trackerOperation.addLog( host.getHostname() + " is removed from configuration files" );
                }
            }

            try
            {
                manager.saveConfig( config );
                trackerOperation.addLog(
                        AccumuloClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            catch ( ClusterException e )
            {
                e.printStackTrace();
            }
        }
        catch ( CommandException e )
        {
            LOGGER.error( "Error uninstalling product on node.", e );
        }
        return result;
    }
}
