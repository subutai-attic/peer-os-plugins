package org.safehaus.subutai.plugin.accumulo.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.accumulo.impl.AccumuloImpl;
import org.safehaus.subutai.plugin.accumulo.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.accumulo.impl.Commands;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
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
        //        Preconditions.checkNotNull( nodeType, "Node type is null." );

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

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg =
                    String.format( "Environment with id: %s doesn't exist.", config.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
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
                case UNINSTALL:
                    result = uninstallProductOnNode( host, nodeType );
                    break;
                case INSTALL:
                    result = installProductOnNode( host, nodeType );
                    break;
                case DESTROY:
                    if ( hostname == null || "".equals( hostname ) )
                    {
                        result = destroyNode( clusterName, hostname );
                    }
                    else
                    {
                        trackerOperation.addLogFailed( "Failed to destroy null container. Provide hostname." );
                    }
                    break;
                case ADD:
                    if ( hostname == null || "".equals( hostname ) )
                    {
                        result = addNode( clusterName, nodeType );
                    }
                    else
                    {
                        result = addNode( clusterName, hostname, nodeType );
                    }
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


    private CommandResult destroyNode( final String clusterName, final String containerHostname )
    {
        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg =
                    String.format( "Environment with id: %s doesn't exists.", config.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
        Set<ContainerHost> environmentHosts;
        try
        {
            environmentHosts = environment.getContainerHostsByIds( config.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Some containers are not accessible from environment." );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
        ContainerHost targetHost = null;
        for ( final ContainerHost environmentHost : environmentHosts )
        {
            if ( environmentHost.getHostname().equalsIgnoreCase( containerHostname ) )
            {
                targetHost = environmentHost;
                break;
            }
        }
        if ( targetHost == null )
        {
            trackerOperation.addLogFailed( String.format( "%s is not found in environment", containerHostname ) );
            return null;
        }
        else
        {
            if ( config.getSlaves().contains( targetHost.getId() ) )
            {
                return uninstallProductOnNode( targetHost, NodeType.ACCUMULO_TABLET_SERVER );
            }
            else if ( config.getTracers().contains( targetHost.getId() ) )
            {
                return uninstallProductOnNode( targetHost, NodeType.ACCUMULO_TRACER );
            }
        }
        trackerOperation.addLogDone( "" );
        return null;
    }


    /**
     * Adds specified container to existing cluster. Checks if container already configured in cluster, if not, adds
     * environment container first to hadoop cluster and zookeeper cluster, finally installs accumulo and triggers
     * cluster configuration with new environment container.
     *
     * @param clusterName - user specified clusterName
     * @param containerName - user specified environment container name
     * @param nodeType - new node role
     *
     * @return - trackerOperationViewId
     */
    private CommandResult addNode( final String clusterName, final String containerName, final NodeType nodeType )
    {
        CommandResult result;

        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
        ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( config.getZookeeperClusterName() );

        Environment accumuloEnvironment;
        try
        {
            accumuloEnvironment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exist", config.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }
        ContainerHost environmentContainerHost;
        try
        {
            environmentContainerHost = accumuloEnvironment.getContainerHostByHostname( containerName );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Container with name: %s doesn't exists in environment: %s", containerName,
                    accumuloEnvironment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }

        //check if passed container is valid container
        if ( environmentContainerHost == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "There is no environment contain with containerName: %s", containerName ) );
            return null;
        }

        //check if container already in accumulo cluster
        if ( config.getAllNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation.addLogFailed(
                    String.format( "Environment container: %s already configured in accumulo cluster.",
                            containerName ) );
        }

        //check if container already running hadoop if not install it
        if ( !hadoopClusterConfig.getAllNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation
                    .addLog( String.format( "Container: %s isn't configured in hadoop cluster.", containerName ) );
            trackerOperation.addLog( String.format( "Adding container: %s to hadoop cluster: %s", containerName,
                    hadoopClusterConfig.getClusterName() ) );
            hadoop.addNode( hadoopClusterConfig.getClusterName(), containerName );
        }

        //check if container already running zookeeper if not install it
        if ( !zookeeperClusterConfig.getNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation
                    .addLog( String.format( "Container: %s isn't configured in zookeeper cluster.", containerName ) );
            trackerOperation.addLog( String.format( "Adding container %s to zookeeper cluster: %s", containerName,
                    zookeeperClusterConfig.getClusterName() ) );
            zookeeper.addNode( zookeeperClusterConfig.getClusterName(), containerName );
        }


        //installing accumulo package
        //initializing accumulo cluster configuration with new node
        result = installProductOnNode( environmentContainerHost, nodeType );
        trackerOperation.addLogDone( "" );
        return result;
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

        Environment accumuloEnvironment;
        try
        {
            accumuloEnvironment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.", config.getEnvironmentId() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return null;
        }

        //get environment containers
        List<ContainerHost> environmentContainers = new ArrayList<>( accumuloEnvironment.getContainerHosts() );

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
                additionalNode = accumuloEnvironment.getContainerHostById( freshContainerHostIds.iterator().next() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                String msg = String.format( "Container host with id: %s is not found in environment: %s",
                        freshContainerHostIds.iterator().next().toString(), accumuloEnvironment.getName() );
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

            result = host.execute( Commands.getInstallCommand(
                    Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() ).withTimeout( 3600 ) );
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

            result = host.execute( new RequestBuilder(
                    Commands.uninstallCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_NAME
                            .toLowerCase() ) );
            if ( result.hasSucceeded() )
            {
                switch ( nodeType )
                {
                    case ACCUMULO_TRACER:
                        config.getTracers().remove( host.getId() );
                        break;
                    case ACCUMULO_TABLET_SERVER:
                        config.getSlaves().remove( host.getId() );
                        break;
                }

                // Configure all nodes again
                try
                {
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
                    manager.unsubscribeFromAlerts( environment );
                    new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, environment );
                }
                catch ( ClusterConfigurationException | MonitorException e )
                {
                    LOGGER.error( "Error configuring nodes after uninstall operation.", e );
                }


                manager.getPluginDAO().saveInfo( AccumuloClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLog(
                        AccumuloClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not uninstall " + AccumuloClusterConfig.PRODUCT_KEY + " from node " + hostname );
            }
        }
        catch ( CommandException e )
        {
            LOGGER.error( "Error uninstalling product on node.", e );
        }
        return result;
    }
}
