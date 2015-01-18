package org.safehaus.subutai.plugin.accumulo.impl.handler;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
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
        Preconditions.checkNotNull( nodeType, "Node type is null." );

        this.hostname = hostname;
        this.clusterName = clusterName;
        this.hadoop = hadoop;
        this.zookeeper = zookeeper;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public NodeOperationHandler( final AccumuloImpl manager, final Hadoop hadoopManager, final Zookeeper zkManager,
                                 final String clusterName, final NodeOperationType nodeOperationType,
                                 final NodeType nodeType )
    {
        super( manager, manager.getCluster( clusterName ) );
        Preconditions.checkNotNull( manager, "Accumulo manager is null." );
        Preconditions.checkNotNull( hadoopManager, "Hadoop manager is null." );
        Preconditions.checkNotNull( zkManager, "Zookeeper manager is null." );
        Preconditions.checkNotNull( clusterName, "Accumulo clusterName is null." );
        Preconditions.checkNotNull( nodeOperationType, "Node operation type is null." );
        Preconditions.checkNotNull( nodeType, "Node type is null." );

        this.clusterName = clusterName;
        this.hadoop = hadoopManager;
        this.zookeeper = zkManager;
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

        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        ContainerHost host = environment.getContainerHostByHostname( hostname );

        if ( host == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "No container in environment: %s with hostname %s", environment.getName(),
                            hostname ) );
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
                    result = host.execute( new RequestBuilder( Commands.stopCommand ) );
                    break;
                case STATUS:
                    result = host.execute( new RequestBuilder( Commands.statusCommand ) );
                    break;
                case UNINSTALL:
                    result = uninstallProductOnNode( host, nodeType );
                    break;
                case INSTALL:
                    result = installProductOnNode( host, nodeType );
                    break;
                case DESTROY:
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
            assert result != null;
            trackerOperation.addLogDone( result.getStdOut() );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
            LOGGER.error( String.format( "Command failed" ), e );
        }
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
        CommandResult result = null;

        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
        ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( config.getZookeeperClusterName() );

        Environment accumuloEnvironment =
                manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        ContainerHost environmentContainerHost = accumuloEnvironment.getContainerHostByHostname( containerName );
        if ( environmentContainerHost == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "There is no environment contain with containerName: %s", containerName ) );
            return null;
        }
        if ( config.getAllNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation.addLogFailed(
                    String.format( "Environment container: %s already configured in accumulo cluster.",
                            containerName ) );
        }
        if ( !hadoopClusterConfig.getAllNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation
                    .addLog( String.format( "Container: %s isn't configured in hadoop cluster.", containerName ) );
            trackerOperation.addLog( String.format( "Adding container: %s to hadoop cluster: %s", containerName,
                    hadoopClusterConfig.getClusterName() ) );
            hadoop.addNode( hadoopClusterConfig.getClusterName(), containerName );
        }
        if ( !zookeeperClusterConfig.getNodes().contains( environmentContainerHost.getId() ) )
        {
            trackerOperation
                    .addLog( String.format( "Container: %s isn't configured in zookeeper cluster.", containerName ) );
            trackerOperation.addLog( String.format( "Adding container %s to zookeeper cluster: %s", containerName,
                    zookeeperClusterConfig.getClusterName() ) );
            zookeeper.addNode( zookeeperClusterConfig.getClusterName(), containerName );
        }


        try
        {
            result = environmentContainerHost.execute( new RequestBuilder(
                    Commands.installCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() )
                    .withTimeout( 1800 ) );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog(
                        AccumuloClusterConfig.PRODUCT_KEY + " is installed on node " + environmentContainerHost
                                .getHostname() );
            }
            else
            {
                trackerOperation.addLogFailed(
                        AccumuloClusterConfig.PRODUCT_KEY + " is not installed on node " + environmentContainerHost
                                .getTemplateName() );
            }
        }
        catch ( CommandException e )
        {
            LOGGER.error( String.format( "Error installing accumulo in containerHost: %s", containerName ) );
        }
        try
        {
            switch ( nodeType )
            {
                case ACCUMULO_TRACER:
                    config.getTracers().add( environmentContainerHost.getId() );
                    break;
                case ACCUMULO_TABLET_SERVER:
                    config.getSlaves().add( environmentContainerHost.getId() );
                    break;
            }
            new ClusterConfiguration( manager, trackerOperation )
                    .configureCluster( accumuloEnvironment, config, zookeeperClusterConfig );
        }
        catch ( ClusterConfigurationException e )
        {
            LOGGER.error( e.getMessage() );
        }

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
        CommandResult result = null;

        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
        ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( config.getZookeeperClusterName() );

        Environment accumuloEnvironment =
                manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );

        //get environment containers
        Set<ContainerHost> environmentContainers = accumuloEnvironment.getContainerHosts();

        //get containers id
        Set<UUID> accumuloEnvDiffContainers = new HashSet<>();
        for ( final ContainerHost containerHost : environmentContainers )
        {
            accumuloEnvDiffContainers.add( containerHost.getId() );
        }

        //remove containers which are not in current accumulo cluster
        accumuloEnvDiffContainers.removeAll( config.getAllNodes() );

        //add new node if all environment containers are already in accumulo cluster
        //otherwise proceed with existing environment container
        ContainerHost additionalNode = null;
        if ( accumuloEnvDiffContainers.isEmpty() )
        {
            //add new node and get its uuid
            hadoop.addNode( hadoopClusterConfig.getClusterName() );
            HadoopClusterConfig freshHadoopClusterConfig = hadoop.getCluster( hadoopClusterConfig.getClusterName() );
            List<UUID> freshContainerHostIds = freshHadoopClusterConfig.getAllNodes();
            freshContainerHostIds.removeAll( hadoopClusterConfig.getAllNodes() );
            additionalNode = accumuloEnvironment.getContainerHostById( freshContainerHostIds.get( 0 ) );
        }
        else
        {
            additionalNode = accumuloEnvironment.getContainerHostById( accumuloEnvDiffContainers.iterator().next() );
        }

        if ( additionalNode == null )
        {
            trackerOperation.addLogFailed( "Couldn't create container host." );
            return null;
        }

        if ( !hadoopClusterConfig.getAllNodes().contains( additionalNode.getId() ) )
        {
            trackerOperation.addLog( String.format( "Container: %s isn't configured in hadoop cluster.",
                    additionalNode.getHostname() ) );
            trackerOperation.addLog(
                    String.format( "Adding container: %s to hadoop cluster: %s", additionalNode.getHostname(),
                            hadoopClusterConfig.getClusterName() ) );
            hadoop.addNode( hadoopClusterConfig.getClusterName(), additionalNode.getHostname() );
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


        try
        {
            result = additionalNode.execute( new RequestBuilder(
                    Commands.installCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() )
                    .withTimeout( 1800 ) );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog(
                        AccumuloClusterConfig.PRODUCT_KEY + " is installed on node " + additionalNode.getHostname() );
            }
            else
            {
                trackerOperation.addLogFailed(
                        AccumuloClusterConfig.PRODUCT_KEY + " is not installed on node " + additionalNode
                                .getTemplateName() );
            }
        }
        catch ( CommandException e )
        {
            LOGGER.error(
                    String.format( "Error installing accumulo in containerHost: %s", additionalNode.getHostname() ) );
        }
        try
        {
            switch ( nodeType )
            {
                case ACCUMULO_TRACER:
                    config.getTracers().add( additionalNode.getId() );
                    break;
                case ACCUMULO_TABLET_SERVER:
                    config.getSlaves().add( additionalNode.getId() );
                    break;
            }
            new ClusterConfiguration( manager, trackerOperation )
                    .configureCluster( accumuloEnvironment, config, zookeeperClusterConfig );
        }
        catch ( ClusterConfigurationException e )
        {
            LOGGER.error( e.getMessage() );
        }

        return result;
    }


    private CommandResult installProductOnNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;
        try
        {

            result = host.execute( new RequestBuilder(
                    Commands.installCommand + Common.PACKAGE_PREFIX + AccumuloClusterConfig.PRODUCT_KEY.toLowerCase() )
                    .withTimeout( 3600 ) );
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
                    Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID(
                            hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() );
                    new ClusterConfiguration( manager, trackerOperation ).configureCluster( environment, config,
                            zookeeper.getCluster( config.getZookeeperClusterName() ) );
                }
                catch ( ClusterConfigurationException e )
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
                    Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID(
                            hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() );
                    new ClusterConfiguration( manager, trackerOperation ).configureCluster( environment, config,
                            zookeeper.getCluster( config.getZookeeperClusterName() ) );
                }
                catch ( ClusterConfigurationException e )
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
