package io.subutai.plugin.galera.impl.handler;


import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.subutai.common.command.CommandException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.Node;
import io.subutai.common.environment.Topology;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Peer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.peer.PeerId;
import io.subutai.common.peer.ResourceHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.hub.share.resource.PeerGroupResources;
import io.subutai.plugin.galera.api.GaleraClusterConfig;
import io.subutai.plugin.galera.impl.ClusterConfiguration;
import io.subutai.plugin.galera.impl.GaleraImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<GaleraImpl, GaleraClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private GaleraClusterConfig config;


    public ClusterOperationHandler( final GaleraImpl manager, final GaleraClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( GaleraClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType operationType )
    {
        switch ( operationType )
        {
            case INSTALL_OVER_ENV:
                break;
            case UNINSTALL:
                break;
        }
    }


    /**
     * Configures container hosts for solr cluster
     */
    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Configuring cluster..." );

        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( environment, config, trackerOperation );
            clusterSetupStrategy.setup();
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            String msg = String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() );
            trackerOperation.addLogFailed( msg );
            throw new RuntimeException( msg );
        }
        trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
    }


    @Override
    public void destroyCluster()
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterConfiguration configuration = new ClusterConfiguration( manager, trackerOperation );
            configuration.deleteConfiguration( config, environment );

            manager.getPluginDAO().deleteInfo( GaleraClusterConfig.PRODUCT_KEY, config.getClusterName() );
            trackerOperation.addLogDone( "Cluster removed from database" );

        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Can not find environment: %s", e.getMessage() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg );
        }
        catch ( ClusterConfigurationException e )
        {
            String msg = String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() );
            trackerOperation.addLogFailed( msg );
            LOG.error( msg );
        }
    }


    @Override
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
            case ADD:
                addNode();
                break;
        }
    }


    private void addNode()
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        EnvironmentContainerHost newNode = null;
        try
        {
            Set<EnvironmentContainerHost> newlyCreatedContainers = new HashSet<>();
            /**
             * first check if there are containers in environment that is not being used in hadoop cluster,
             * if yes, then do not create new containers.
             */
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            int numberOfContainersNotBeingUsed = 0;
            boolean allContainersNotBeingUsed = false;

            List<Integer> containersIndex = Lists.newArrayList();
            for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
            {
                String numbers = containerHost.getContainerName().replace( "Container", "" ).trim();
                String contId = numbers.split( "-" )[0];
                containersIndex.add( Integer.parseInt( contId ) );

                if ( !config.getNodes().contains( containerHost.getId() ) )
                {
                    if ( containerHost.getTemplateName().equals( GaleraClusterConfig.TEMPLATE_NAME.toLowerCase() ) )
                    {
                        if ( !isThisNodeUsedInOtherClusters( containerHost.getId() ) )
                        {
                            allContainersNotBeingUsed = true;
                            numberOfContainersNotBeingUsed++;
                        }
                    }
                }
            }


            if ( ( !allContainersNotBeingUsed ) | ( numberOfContainersNotBeingUsed < 1 ) )
            {
                String containerName = "Container" + String.valueOf( Collections.max( containersIndex ) + 1 );
                final int newNodes = 1 - numberOfContainersNotBeingUsed;
                Set<Node> nodeGroups = new HashSet<>();
                PeerGroupResources groupResources = manager.getPeerManager().getPeerGroupResources();
                for ( Peer peer : environment.getPeers() )
                {
                    try
                    {
                        groupResources.getResources().add( peer
                                .getResourceLimits( new PeerId( manager.getPeerManager().getLocalPeer().getId() ) ) );
                    }
                    catch ( PeerException e )
                    {
                        //ignore
                    }
                }

                for ( int i = 0; i < newNodes; i++ )
                {
                    ResourceHost resourceHost =
                            manager.getPeerManager().getLocalPeer().getResourceHosts().iterator().next();

                    Node nodeGroup =
                            new Node( containerName, containerName, ContainerSize.LARGE, resourceHost.getPeerId(),
                                    resourceHost.getId(),
                                    manager.getTemplateManager().getTemplateByName( GaleraClusterConfig.TEMPLATE_NAME )
                                           .getId() );

                    nodeGroups.add( nodeGroup );
                }


                if ( numberOfContainersNotBeingUsed > 0 )
                {
                    trackerOperation.addLog(
                            "Using " + numberOfContainersNotBeingUsed + " existing containers and creating" + ( 1
                                    - numberOfContainersNotBeingUsed ) + " containers." );
                }
                else
                {
                    trackerOperation.addLog( "Creating new containers..." );
                }
                Topology topology = new Topology( environment.getName() );

                for ( Node nodeGroup : nodeGroups )
                {
                    topology.addNodePlacement( nodeGroup.getPeerId(), nodeGroup );
                }
                newlyCreatedContainers =
                        environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );

                newNode = newlyCreatedContainers.iterator().next();
                config.getNodes().add( newNode.getId() );
            }
            else
            {
                trackerOperation.addLog( "Using existing containers that are not taking role in cluster" );
                // update cluster configuration on DB
                int count = 0;
                environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( !config.getNodes().contains( containerHost.getId() ) )
                    {
                        if ( count <= 1 )
                        {
                            //                            config.getSlaves().add( containerHost.getId() );
                            newNode = containerHost;
                            count++;
                            newlyCreatedContainers.add( containerHost );
                        }
                    }
                }
            }

            config.getNodes().add( newNode.getId() );

            ClusterConfiguration configurator = new ClusterConfiguration( manager, trackerOperation );

            GaleraClusterConfig updatedConfig = configurator
                    .addNode( environmentManager.loadEnvironment( config.getEnvironmentId() ), config, newNode );

            manager.saveConfig( updatedConfig );

            trackerOperation.addLogDone( "Finished." );
        }
        catch ( EnvironmentNotFoundException | EnvironmentModificationException | PeerException e )
        {
            logExceptionWithMessage( "Error executing operations with environment", e );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Could not save cluster configuration", e );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        catch ( ClusterConfigurationException e )
        {
            e.printStackTrace();
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOG.error( message, e );
        trackerOperation.addLogFailed( message );
    }


    private boolean isThisNodeUsedInOtherClusters( String id )
    {
        List<GaleraClusterConfig> configs = manager.getClusters();
        for ( GaleraClusterConfig config1 : configs )
        {
            if ( config1.getNodes().contains( id ) )
            {
                return true;
            }
        }
        return false;
    }
}
