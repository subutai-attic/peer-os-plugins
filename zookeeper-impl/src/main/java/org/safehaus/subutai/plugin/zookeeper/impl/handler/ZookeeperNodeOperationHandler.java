package org.safehaus.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentManagerException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.api.SetupType;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


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


    public ZookeeperNodeOperationHandler( final ZookeeperImpl manager, final ZookeeperClusterConfig config,
                                          final String hostname, NodeOperationType nodeOperationType )
    {
        super( manager, config );
        this.clusterName = config.getClusterName();
        this.hostname = hostname;
        this.operationType = nodeOperationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( ZookeeperClusterConfig.PRODUCT_NAME,
                String.format( "Running %s operation on %s...", operationType, hostname ) );
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

        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        ContainerHost containerHost = environment.getContainerHostByHostname( hostname );

        if ( containerHost == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }

        try
        {
            List<CommandResult> commandResultList = new ArrayList<>();
            switch ( operationType )
            {
                case START:
                    commandResultList.add( containerHost.execute( new RequestBuilder( Commands.getStartCommand() ) ) );
                    break;
                case STOP:
                    commandResultList.add( containerHost.execute( new RequestBuilder( Commands.getStopCommand() ) ) );
                    break;
                case STATUS:
                    commandResultList.add( containerHost.execute( new RequestBuilder( Commands.getStatusCommand() ) ) );
                    break;
                case ADD:
                    if ( config.getSetupType() == SetupType.OVER_HADOOP
                            | config.getSetupType() == SetupType.OVER_ENVIRONMENT )
                    {
                        commandResultList.addAll( addNode( hostname ) );
                    }
                    else if ( config.getSetupType() == SetupType.STANDALONE )
                    {
                        addNode();
                    }
                    else
                    {
                        trackerOperation.addLogFailed( "Not supported SetupType" );
                        return;
                    }
                    break;
                case DESTROY:
                    if ( config.getSetupType() == SetupType.OVER_HADOOP
                            || config.getSetupType() == SetupType.OVER_ENVIRONMENT )
                    {
                        commandResultList
                                .add( containerHost.execute( new RequestBuilder( Commands.getUninstallCommand() ) ) );
                        boolean isRemoved = config.getNodes().remove( containerHost.getId() );
                        if ( isRemoved )
                        {
                            manager.getPluginDAO().deleteInfo( config.getProductKey(), config.getClusterName() );
                            manager.getPluginDAO().saveInfo( config.getProductKey(), config.getClusterName(), config );
                        }
                    }
                    else
                    {
                        destroyNode( containerHost );
                    }
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
    }


    public void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        if ( config.getSetupType() == SetupType.OVER_HADOOP )
        {
            HadoopClusterConfig hadoopCluster = manager.getHadoopManager().
                    getCluster( config.getHadoopClusterName() );
            Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopCluster.getEnvironmentId() );

            trackerOperation.addLog( "Validating node addition." );
            List<UUID> hadoopContainerHostIds = hadoopCluster.getAllNodes();
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
                manager.getHadoopManager().addNode( config.getHadoopClusterName() );
                trackerOperation.addLogFailed( "Adding hadoop node." );
            }
            ContainerHost newNode = hadoopEnvironment.getContainerHostById( hadoopContainerHostIds.get( 0 ) );
            addNode( newNode.getHostname() );
        }
        else
        {

            NodeGroup nodeGroup = new NodeGroup();
            nodeGroup.setName( ZookeeperClusterConfig.PRODUCT_NAME );
            nodeGroup.setLinkHosts( true );
            nodeGroup.setExchangeSshKeys( true );
            nodeGroup.setDomainName( Common.DEFAULT_DOMAIN_NAME );
            nodeGroup.setTemplateName( config.getTemplateName() );
            nodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );
            nodeGroup.setNumberOfNodes( 1 );

            GsonBuilder builder = new GsonBuilder();
            Gson gson = builder.create();
            String ngJSON = gson.toJson( nodeGroup );

            trackerOperation.addLog( "Adding node to environment." );
            try
            {
                environmentManager.createAdditionalContainers( config.getEnvironmentId(), ngJSON, localPeer );
                Environment environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
                // update cluster configuration on DB
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( !config.getNodes().contains( containerHost.getId() ) )
                    {
                        config.getNodes().add( containerHost.getId() );
                    }
                }
                manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );

                ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
                try
                {
                    configurator.configureCluster( config,
                            environmentManager.getEnvironmentByUUID( config.getEnvironmentId() ) );
                }
                catch ( ClusterConfigurationException e )
                {
                    LOGGER.error( "Error while adding node to environment: " + environment.getId(), e );
                }
            }
            catch ( EnvironmentBuildException e )
            {
                LOGGER.error( "Couldn't create additional container.", e );
            }
        }
    }


    public List<CommandResult> addNode( String hostName )
    {
        Preconditions.checkNotNull( hostName, "Hostname is null" );

        List<CommandResult> commandResultList = new ArrayList<>();
        Environment zookeeperEnvironment = manager.getEnvironmentManager().
                getEnvironmentByUUID( config.getEnvironmentId() );
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

        try
        {
            String command = Commands.getInstallCommand();
            if ( !newNode.isConnected() )
            {
                trackerOperation.addLogFailed( String.format( "Host %s is not connected. Aborting", hostName ) );
                return commandResultList;
            }
            CommandResult commandResult = executeCommand( newNode, command );
            commandResultList.add( commandResult );
            if ( !commandResult.hasSucceeded() )
            {
                trackerOperation.addLogFailed( String.format( "Command %s failed on %s", command, hostName ) );
                return commandResultList;
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
        return commandResultList;
    }


    public void destroyNode( ContainerHost host )
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        try
        {
            ZookeeperClusterConfig config = manager.getCluster( clusterName );
            environmentManager.removeContainer( config.getEnvironmentId(), host.getId() );
            config.getNodes().remove( host.getId() );
            manager.getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            try
            {
                configurator.configureCluster( config,
                        environmentManager.getEnvironmentByUUID( config.getEnvironmentId() ) );
            }
            catch ( ClusterConfigurationException e )
            {
                e.printStackTrace();
            }
            trackerOperation.addLog( String.format( "Cluster information is updated" ) );
            trackerOperation.addLogDone( String.format( "Container %s is removed from cluster", host.getHostname() ) );
        }
        catch ( EnvironmentManagerException e )
        {
            e.printStackTrace();
        }
    }
}
