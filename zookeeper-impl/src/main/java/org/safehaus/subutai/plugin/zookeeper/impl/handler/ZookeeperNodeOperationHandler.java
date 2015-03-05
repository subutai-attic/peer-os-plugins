package org.safehaus.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.api.CommandType;
import org.safehaus.subutai.plugin.zookeeper.api.SetupType;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to just one node.
 *
 * TODO: add nodes and delete node operation should be implemented.
 */
public class ZookeeperNodeOperationHandler extends AbstractPluginOperationHandler<ZookeeperImpl, ZookeeperClusterConfig>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperNodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    CommandUtil commandUtil;


    public ZookeeperNodeOperationHandler( final ZookeeperImpl manager, final ZookeeperClusterConfig config,
                                          NodeOperationType nodeOperationType )
    {
        super( manager, config );
        this.clusterName = config.getClusterName();
        this.operationType = nodeOperationType;
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
            Environment environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
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
                    commandResultList
                            .add( containerHost.execute( new RequestBuilder( Commands.getStartCommand() ).daemon() ) );
                    break;
                case STOP:
                    commandResultList.add( containerHost.execute( new RequestBuilder( Commands.getStopCommand() ) ) );
                    break;
                case STATUS:
                    commandResultList.add( containerHost.execute( new RequestBuilder( Commands.getStatusCommand() ) ) );
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
                    getManager().unsubscribeFromAlerts( environment );
                    break;
            }
            logResults( trackerOperation, commandResultList );
        }
        catch ( CommandException | MonitorException e )
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
                    config.getEnvironmentId().toString() ), e );
            trackerOperation.addLogFailed( String.format( "Couldn't retrieve environment with id: %s",
                    config.getEnvironmentId().toString() ) );
        }
    }


    public void addNode()
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        ZookeeperClusterConfig config = manager.getCluster( clusterName );

        if ( config.getSetupType() == SetupType.OVER_HADOOP )
        {
            HadoopClusterConfig hadoopCluster = manager.getHadoopManager().
                    getCluster( config.getHadoopClusterName() );
            List<UUID> hadoopContainerHostIds = hadoopCluster.getAllNodes();
            try
            {
                Environment hadoopEnvironment = environmentManager.findEnvironment( hadoopCluster.getEnvironmentId() );

                trackerOperation.addLog( "Validating node addition." );

                List<UUID> zookeeperContainerHosts = new ArrayList<>( config.getNodes() );
                //Check does hadoop have nodes where zk is not installed
                for ( int i = 0; i < hadoopContainerHostIds.size(); i++ )
                {
                    UUID hadoopContainerId = hadoopContainerHostIds.get( i );
                    if ( zookeeperContainerHosts.contains( hadoopContainerId ) )
                    {
                        hadoopContainerHostIds.remove( i-- );
                    }
                }
                if ( hadoopContainerHostIds.isEmpty() )
                {
                    List<UUID> hadoopNodes =
                            manager.getHadoopManager().getCluster( config.getHadoopClusterName() ).getAllNodes();
                    manager.getHadoopManager().addNode( config.getHadoopClusterName() );
                    trackerOperation.addLog( "Adding hadoop node." );
                    hadoopNodes.removeAll(
                            manager.getHadoopManager().getCluster( config.getHadoopClusterName() ).getAllNodes() );
                    if ( hadoopNodes.isEmpty() )
                    {
                        trackerOperation.addLogFailed( "Failed to add container to hadoop environment." );
                        return;
                    }
                    hadoopContainerHostIds.addAll( hadoopNodes );
                }
                ContainerHost newNode =
                        hadoopEnvironment.getContainerHostById( hadoopContainerHostIds.iterator().next() );
                addNode( newNode.getHostname() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOGGER.error( String.format( "Container host not found for one of these ids: %s",
                        hadoopContainerHostIds.toString() ), e );
                trackerOperation.addLogFailed( String.format( "Container host not found for one of these ids: %s",
                        hadoopContainerHostIds.toString() ) );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( String.format( "Got a blank environment for id: %s, right place to start from scratch.",
                        config.getEnvironmentId().toString() ), e );
                trackerOperation.addLogFailed( String.format( "Couldn't retrieve environment with id: %s",
                        hadoopCluster.getEnvironmentId().toString() ) );
            }
        }
        else if ( config.getSetupType() == SetupType.OVER_ENVIRONMENT )
        {
            try
            {
                Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                List<ContainerHost> envContainerHosts = new ArrayList<>( environment.getContainerHosts() );
                trackerOperation.addLog( "Validating node addition." );

                List<UUID> zookeeperContainerHosts = new ArrayList<>( config.getNodes() );
                //Check does environment have nodes where zk is not installed
                for ( int i = 0; i < envContainerHosts.size(); i++ )
                {
                    UUID hadoopContainerId = envContainerHosts.get( i ).getId();
                    if ( zookeeperContainerHosts.contains( hadoopContainerId ) )
                    {
                        envContainerHosts.remove( i-- );
                    }
                }
                Set<ContainerHost> johnnyRawSet = new HashSet<>( envContainerHosts );
                //if envHost is not in zoo cluster add it else create new one.
                if ( envContainerHosts.isEmpty() )
                {
                    NodeGroup nodeGroup =
                            new NodeGroup( ZookeeperClusterConfig.PRODUCT_NAME, ZookeeperClusterConfig.TEMPLATE_NAME, 1,
                                    1, 1, new PlacementStrategy( "ROUND_ROBIN" ) );

                    Topology topology = new Topology();
                    topology.addNodeGroupPlacement( manager.getPeerManager().getLocalPeer(), nodeGroup );
                    johnnyRawSet.addAll( environment.growEnvironment( topology, false ) );
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
                        config.getEnvironmentId().toString() ), e );
                trackerOperation.addLogFailed( String.format( "Couldn't retrieve environment with id: %s",
                        config.getEnvironmentId().toString() ) );
            }
            catch ( EnvironmentModificationException e )
            {
                LOGGER.error( "Error breeding environment", e );
                trackerOperation.addLogFailed( "Error breeding environment" );
            }
        }
    }


    public List<CommandResult> addNode( String hostName )
    {
        Preconditions.checkNotNull( hostName, "Hostname is null" );
        ZookeeperClusterConfig config = manager.getCluster( clusterName );

        List<CommandResult> commandResultList = new ArrayList<>();
        try
        {
            Environment zookeeperEnvironment =
                    manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            ContainerHost newNode = zookeeperEnvironment.getContainerHostByHostname( hostName );
            if ( config.getNodes().contains( newNode.getId() ) && config.getSetupType() == SetupType.OVER_ENVIRONMENT )
            {
                trackerOperation
                        .addLogFailed( String.format( "%s already in zookeeper environment.", newNode.getHostname() ) );
                return commandResultList;
            }
            if ( config.getSetupType() == SetupType.OVER_HADOOP )
            {
                HadoopClusterConfig hadoopCluster = manager.getHadoopManager().
                        getCluster( config.getHadoopClusterName() );
                if ( !hadoopCluster.getAllNodes().contains( newNode.getId() ) )
                {
                    trackerOperation.addLogFailed(
                            String.format( "%s node doesn't have hadoop installed, add it to hadoop environment.",
                                    newNode.getHostname() ) );
                    return commandResultList;
                }
            }

            if ( !newNode.isConnected() )
            {
                trackerOperation.addLogFailed( String.format( "Host %s is not connected. Aborting", hostName ) );
                return commandResultList;
            }
            CommandResult commandResult;
            try
            {
                commandResult = newNode.execute( new RequestBuilder( Commands.getStatusCommand() ).withTimeout( 30 ) );
                if ( commandResult.getStdErr().contains( "unrecognized service" ) )
                {
                    newNode.execute( new RequestBuilder( Commands.getInstallCommand() ).withTimeout( 120 ) );
                }
            }
            catch ( CommandException e )
            {
                trackerOperation.addLogFailed( "Couldn't configure host for zookeeper cluster" );
                LOGGER.error( "Couldn't configure host for zookeeper cluster", e );
                return Collections.emptyList();
            }

            // check if cluster is already running, then newly added node should be started automatically.
            RequestBuilder checkClusterIsRunning = new RequestBuilder( manager.getCommand( CommandType.STATUS ) );
            ContainerHost host = zookeeperEnvironment.getContainerHostById( config.getNodes().iterator().next() );
            try
            {
                CommandResult result = commandUtil.execute( checkClusterIsRunning, host );
                if ( result.hasSucceeded() )
                {
                    if ( result.getStdOut().contains( "pid" ) )
                    {
                        commandUtil.execute( new RequestBuilder( manager.getCommand( CommandType.START ) ).daemon(),
                                newNode );
                    }
                }
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }


            config.getNodes().add( newNode.getId() );
            new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, zookeeperEnvironment );
            trackerOperation.addLog( "Updating cluster information..." );
            manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
            manager.subscribeToAlerts( newNode );
        }
        catch ( MonitorException | ClusterConfigurationException e )
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
            LOGGER.error(
                    String.format( "Environment with id: %s doesn't exist", config.getEnvironmentId().toString() ), e );
            trackerOperation.addLogFailed(
                    String.format( "Environment with id: %s doesn't exist", config.getEnvironmentId().toString() ) );
        }
        return commandResultList;
    }


    public void destroyNode( ContainerHost host )
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
                environmentManager.destroyContainer( host, true, true );
                config.getNodes().remove( host.getId() );
                manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
                try
                {
                    configurator.configureCluster( config,
                            environmentManager.findEnvironment( config.getEnvironmentId() ) );
                }
                catch ( ClusterConfigurationException e )
                {
                    e.printStackTrace();
                }
                trackerOperation.addLog( String.format( "Cluster information is updated" ) );
                trackerOperation.addLogDone(
                        String.format( "Container %s is removed from cluster", host.getHostname() ) );
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
                trackerOperation
                        .addLogFailed(
                                String.format( "Couldn't find environment with id: %s", host.getEnvironmentId() ) );
            }
        }
    }


    private void removeNode( final ContainerHost host )
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        ZookeeperClusterConfig config = manager.getCluster( clusterName );
        config.getNodes().remove( host.getId() );
        manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        // configure cluster again
        ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
        try
        {
            configurator.configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            e.printStackTrace();
        }
        trackerOperation.addLog( String.format( "Cluster information is updated" ) );
        trackerOperation.addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
    }


    private void uninstallNode( final ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            host.execute( new RequestBuilder( Commands.getStopCommand() ) );
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
