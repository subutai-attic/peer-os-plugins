package io.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.Node;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerSize;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.ClusterConfiguration;
import io.subutai.plugin.zookeeper.impl.Commands;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;


/**
 * This class handles operations that are related to just one node.
 */
public class ZookeeperNodeOperationHandler extends AbstractPluginOperationHandler<ZookeeperImpl, ZookeeperClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( ZookeeperNodeOperationHandler.class.getName() );

    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperNodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private CommandUtil commandUtil;
    private TemplateManager templateManager;


    public ZookeeperNodeOperationHandler( final ZookeeperImpl manager, final TemplateManager templateManager,
                                          final ZookeeperClusterConfig config, NodeOperationType nodeOperationType )
    {
        super( manager, config );
        this.clusterName = config.getClusterName();
        this.operationType = nodeOperationType;
        this.templateManager = templateManager;
        this.trackerOperation = manager.getTracker().createTrackerOperation( ZookeeperClusterConfig.PRODUCT_NAME,
                String.format( "Running %s operation on %s...", operationType, hostname ) );
        commandUtil = new CommandUtil();
    }


    public ZookeeperNodeOperationHandler( final ZookeeperImpl manager, final String clusterName, final String hostname,
                                          NodeOperationType operationType )
    {

        super( manager, clusterName );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( ZookeeperClusterConfig.PRODUCT_NAME,
                String.format( "Running %s operation on %s...", operationType, hostname ) );
        commandUtil = new CommandUtil();
    }


    @Override
    public void run()
    {
        ZookeeperClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ContainerHost containerHost = null;
            if ( hostname != null )
            {
                containerHost = environment.getContainerHostByHostname( hostname );
            }

            if ( containerHost == null )
            {
                if ( operationType != NodeOperationType.ADD )
                {
                    trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
                    return;
                }
            }

            List<CommandResult> commandResultList = new ArrayList<>();
            switch ( operationType )
            {
                case START:
                    assert containerHost != null;
                    commandResultList.add( containerHost
                            .execute( new RequestBuilder( Commands.getStartZkServerCommand() ).daemon() ) );
                    break;
                case STOP:
                    assert containerHost != null;
                    commandResultList
                            .add( containerHost.execute( new RequestBuilder( Commands.getStopZkServerCommand() ) ) );
                    break;
                case STATUS:
                    assert containerHost != null;
                    commandResultList
                            .add( containerHost.execute( new RequestBuilder( Commands.getStatusZkServerCommand() ) ) );
                    break;
                case ADD:
                    if ( hostname != null )
                    {
                        commandResultList.addAll( addNode( hostname ) );
                    }
                    else
                    {
                        addNode();
                    }
                    break;
                case DESTROY:
                    destroyNode( containerHost );
                    break;
            }
            logResults( trackerOperation, commandResultList );
        }
        catch ( CommandException e )
        {
            LOGGER.error( String.format( "Command failed for operationType: %s", operationType ), e );
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( String.format( "Container host not found with name: %s", hostname ), e );
            trackerOperation.addLogFailed( String.format( "Container host not found with name: %s", hostname ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( String.format( "Got a blank environment for id: %s, right place to start from scratch.",
                    config.getEnvironmentId() ), e );
            trackerOperation.addLogFailed(
                    String.format( "Couldn't retrieve environment with id: %s", config.getEnvironmentId() ) );
        }
    }


    private void addNode()
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        ZookeeperClusterConfig config = manager.getCluster( clusterName );
        if ( config.getSetupType() == SetupType.OVER_ENVIRONMENT )
        {
            try
            {
                Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                List<EnvironmentContainerHost> envContainerHosts = new ArrayList<>( environment.getContainerHosts() );
                trackerOperation.addLog( "Validating node addition." );

                List<String> zookeeperContainerHosts = new ArrayList<>( config.getNodes() );
                //Check does environment have nodes where zk is not installed
                for ( int i = 0; i < envContainerHosts.size(); i++ )
                {
                    String hadoopContainerId = envContainerHosts.get( i ).getId();
                    if ( zookeeperContainerHosts.contains( hadoopContainerId ) )
                    {
                        envContainerHosts.remove( i-- );
                    }
                }
                Set<EnvironmentContainerHost> johnnyRawSet = new HashSet<>( envContainerHosts );
                //if envHost is not in zoo cluster add it else create new one.
                if ( envContainerHosts.isEmpty() )
                {
                    Node nodeGroup = new Node( UUID.randomUUID().toString(), ZookeeperClusterConfig.PRODUCT_NAME,
                            new ContainerQuota( ContainerSize.TINY ), null, null,
                            templateManager.getTemplateByName( ZookeeperClusterConfig.TEMPLATE_NAME ).getId() );
                    Topology topology = new Topology( environment.getName() );
                    /*Blueprint blueprint =
                            new Blueprint( ZookeeperClusterConfig.PRODUCT_NAME, Sets.newHashSet( nodeGroup ) );*/

                    johnnyRawSet.addAll( environmentManager.growEnvironment( environment.getId(), topology, false ) );
                }
                if ( johnnyRawSet.isEmpty() )
                {
                    trackerOperation.addLogFailed( "Couldn't add container host to cluster." );
                    return;
                }
                ContainerHost newNode = johnnyRawSet.iterator().next();
                addNode( newNode.getHostname() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( String.format( "Got a blank environment for id: %s, right place to start from scratch.",
                        config.getEnvironmentId() ), e );
                trackerOperation.addLogFailed(
                        String.format( "Couldn't retrieve environment with id: %s", config.getEnvironmentId() ) );
            }
            catch ( EnvironmentModificationException e )
            {
                LOGGER.error( "Error breeding environment", e );
                trackerOperation.addLogFailed( "Error breeding environment" );
            }
        }
    }


    private List<CommandResult> addNode( String hostName )
    {
        Preconditions.checkNotNull( hostName, "Hostname is null" );
        ZookeeperClusterConfig config = manager.getCluster( clusterName );

        List<CommandResult> commandResultList = new ArrayList<>();
        try
        {
            Environment zookeeperEnvironment =
                    manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ContainerHost newNode = zookeeperEnvironment.getContainerHostByHostname( hostName );
            if ( config.getNodes().contains( newNode.getId() ) && config.getSetupType() == SetupType.OVER_ENVIRONMENT )
            {
                trackerOperation
                        .addLogFailed( String.format( "%s already in zookeeper cluster.", newNode.getHostname() ) );
                return commandResultList;
            }

            if ( !newNode.isConnected() )
            {
                trackerOperation.addLogFailed( String.format( "Host %s is not connected. Aborting", hostName ) );
                return commandResultList;
            }

            config.getNodes().add( newNode.getId() );
            new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, zookeeperEnvironment );
            trackerOperation.addLog( "Updating cluster information..." );
            manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        }
        catch ( ClusterConfigurationException e )
        {
            LOGGER.error( "Error adding node", e );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( String.format( "Container host %s not found", hostName ), e );
            trackerOperation.addLogFailed( String.format( "Container host %s not found", hostName ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( String.format( "Environment with id: %s doesn't exist", config.getEnvironmentId() ), e );
            trackerOperation.addLogFailed(
                    String.format( "Environment with id: %s doesn't exist", config.getEnvironmentId() ) );
        }
        return commandResultList;
    }


    private void destroyNode( ContainerHost host )
    {
        ZookeeperClusterConfig config = manager.getCluster( clusterName );
        if ( config.getSetupType().equals( SetupType.OVER_ENVIRONMENT ) )
        {
            removeNode( host );
        }
        else if ( config.getSetupType().equals( SetupType.OVER_HADOOP ) )
        {
            uninstallNode( host );
        }
        else
        {
            EnvironmentManager environmentManager = manager.getEnvironmentManager();
            try
            {
                config = manager.getCluster( clusterName );
                Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                environmentManager.destroyContainer( environment.getId(), host.getId(), true );
                config.getNodes().remove( host.getId() );
                manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
                try
                {
                    configurator.configureCluster( config,
                            environmentManager.loadEnvironment( config.getEnvironmentId() ) );
                }
                catch ( ClusterConfigurationException e )
                {
                    e.printStackTrace();
                }
                trackerOperation.addLog( "Cluster information is updated" );
                trackerOperation
                        .addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
            }
            catch ( EnvironmentModificationException e )
            {
                LOGGER.error( String.format( "Error clearing database records for environment with id: %s.",
                        host.getEnvironmentId() ) );
                trackerOperation.addLogFailed(
                        String.format( "Error clearing database records for environment with id: %s.",
                                host.getEnvironmentId() ) );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( String.format( "Couldn't find environment with id: %s", host.getEnvironmentId() ), e );
                trackerOperation.addLogFailed(
                        String.format( "Couldn't find environment with id: %s", host.getEnvironmentId() ) );
            }
        }
    }


    private void removeNode( final ContainerHost host )
    {
        try
        {
            EnvironmentManager environmentManager = manager.getEnvironmentManager();
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            ZookeeperClusterConfig config = manager.getCluster( clusterName );
            config.getNodes().remove( host.getId() );
            // configure cluster again
            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            try
            {
                configurator.removeNode( host );
                configurator.configureCluster( config, environment );
            }
            catch ( ClusterConfigurationException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Error during reconfiguration after removing node %s from cluster: %s",
                                host.getHostname(), config.getClusterName() ) );
                LOG.error( String.format( "Error during reconfiguration after removing node %s from cluster: %s",
                        host.getHostname(), config.getClusterName() ), e );
            }
            manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );

            trackerOperation.addLog( "Cluster information is updated" );
            trackerOperation.addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
            LOG.info( String.format( "Container %s is removed from cluster", host.getHostname() ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Environment not found: %s", config.getEnvironmentId() ) );
            LOG.error( String.format( "Environment not found: %s", config.getEnvironmentId() ), e );
        }
    }


    private void uninstallNode( final ContainerHost host )
    {
        CommandResult result;
        try
        {
            host.execute( new RequestBuilder( Commands.getStopZkServerCommand() ) );
            result = host.execute( new RequestBuilder( Commands.getUninstallCommand() ) );
            if ( result.hasSucceeded() )
            {
                ZookeeperClusterConfig config = manager.getCluster( clusterName );
                config.getNodes().remove( host.getId() );
                manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                trackerOperation.addLogDone(
                        ZookeeperClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation.addLogFailed(
                        "Could not uninstall " + ZookeeperClusterConfig.PRODUCT_KEY + " from node " + host
                                .getHostname() );
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }
}
