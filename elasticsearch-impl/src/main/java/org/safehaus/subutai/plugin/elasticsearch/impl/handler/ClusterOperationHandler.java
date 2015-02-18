package org.safehaus.subutai.plugin.elasticsearch.impl.handler;


import java.util.Set;

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
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler
        extends AbstractOperationHandler<ElasticsearchImpl, ElasticsearchClusterConfiguration>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private ElasticsearchClusterConfiguration config;
    CommandUtil commandUtil = new CommandUtil();


    public ClusterOperationHandler( final ElasticsearchImpl manager, final ElasticsearchClusterConfiguration config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( ElasticsearchClusterConfiguration.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case REMOVE:
                removeCluster();
                break;
            case ADD:
                addNode();
                break;

        }
    }


    private void addNode()
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getTemplateName(),
                1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );
        try
        {
            Set<ContainerHost> newNodeSet;
            try
            {
                newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
            }
            catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
            {
                LOG.error( "Could not add new node(s) to environment." );
                throw new ClusterException( e );
            }

            ContainerHost newNode = newNodeSet.iterator().next();

            config.getNodes().add( newNode.getId() );

            manager.saveConfig( config );

            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );
            Environment environment;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                configurator
                        .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
            }
            catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
            {
                LOG.error( "Could not find environment with id {} ", config.getEnvironmentId() );
                throw new ClusterException( e );
            }
            //subscribe to alerts
            try
            {
                manager.subscribeToAlerts( newNode );
            }
            catch ( MonitorException e )
            {
                throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
            }
            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
    }


    public void removeCluster()
    {
        ElasticsearchClusterConfiguration config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        try
        {
            manager.deleteConfig( config );
            trackerOperation.addLogDone( "Cluster removed from database" );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        throw new UnsupportedOperationException();
    }


    private CommandResult executeCommand( ContainerHost containerHost, RequestBuilder command )
    {
        CommandResult result = null;
        try
        {
            result = commandUtil.execute( command, containerHost );
        }
        catch ( CommandException e )
        {
            LOG.error( "Command failed", e );
        }
        return result;
    }


    @Override
    public void setupCluster()
    {
        Environment env;
        try
        {
            env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( manager, trackerOperation ).configureCluster( config, env );
                trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
            }
            catch ( ClusterConfigurationException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Failed to setup Elasticsearch cluster %s : %s", clusterName, e.getMessage() ) );
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void destroyCluster()
    {
        ElasticsearchClusterConfiguration config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        trackerOperation.addLog( "Uninstalling ES..." );
        Set<ContainerHost> esNodes;
        try
        {
            esNodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Error accessing environment containers: %s", e ) );
            return;
        }
        for ( ContainerHost node : esNodes )
        {
            executeCommand( node, manager.getCommands().getUninstallCommand() );
        }
        manager.getPluginDAO().deleteInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName() );

        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            LOG.error( "Failed to unsubscribe from alerts", e );
        }

        trackerOperation.addLogDone( "Cluster destroyed" );
    }
}
