package org.safehaus.subutai.plugin.storm.impl.handler;


import java.util.ArrayList;
import java.util.List;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentDestroyException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.storm.impl.CommandType;
import org.safehaus.subutai.plugin.storm.impl.Commands;
import org.safehaus.subutai.plugin.storm.impl.StormImpl;
import org.safehaus.subutai.plugin.storm.impl.StormService;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * This class handles operations that are related to whole cluster.
 */
public class StormClusterOperationHandler extends AbstractOperationHandler<StormImpl, StormClusterConfiguration>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( StormClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private StormClusterConfiguration config;
    private String hostname;


    public StormClusterOperationHandler( final StormImpl manager,
                                         final StormClusterConfiguration config,
                                         final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( config.getProductKey(),
                String.format( "Running %s operation on %s...", operationType , clusterName ) );
    }


    public StormClusterOperationHandler( final StormImpl manager,
                                         final StormClusterConfiguration config,
                                         final String hostname,
                                         final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.hostname = hostname;
        trackerOperation = manager.getTracker().createTrackerOperation( config.getProductKey(),
                String.format( "Running %s operation on %s...", operationType , clusterName ) );
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        runOperationOnContainers( operationType );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        Environment environment;
        List<CommandResult> commandResultList = new ArrayList<>(  );
        switch ( clusterOperationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
                environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) ) {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.START, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.START, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.START, StormService.SUPERVISOR ) ) );
                }
                break;
            case STOP_ALL:
                environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) ) {
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STOP, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STOP, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STOP, StormService.SUPERVISOR ) ) );
                }
                break;
            case STATUS_ALL:
                environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) ) {
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STATUS, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STATUS, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                        commandResultList.add( executeCommand( containerHost, Commands
                                .make( CommandType.STATUS, StormService.SUPERVISOR ) ) );
                }
                break;
            case ADD:
                addNode();
                break;
            case REMOVE:
                removeCluster();
                break;
        }
        logResults( trackerOperation, commandResultList );
    }


    private void removeCluster()
    {
        StormClusterConfiguration config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        manager.getPluginDAO().deleteInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster removed from database" );
    }


    /**
     * It adds 1 node to cluster.
     */
    public void addNode(){
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        /**
         * first check if there are containers in environment that is not being used in storm cluster,
         * if yes, then do NOT create new containers.
         */
        Environment environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
        boolean allContainersNotBeingUsed = false;
        for ( ContainerHost containerHost : environment.getContainerHosts() ){
            if ( ! config.getAllNodes().contains( containerHost.getId() ) ){
                allContainersNotBeingUsed = true;
            }
        }

        try
        {
            if ( ( ! allContainersNotBeingUsed ) ){

                NodeGroup nodeGroup = new NodeGroup();
                nodeGroup.setName( StormClusterConfiguration.PRODUCT_NAME );
                nodeGroup.setLinkHosts( true );
                nodeGroup.setExchangeSshKeys( true );
                nodeGroup.setDomainName( Common.DEFAULT_DOMAIN_NAME );
                nodeGroup.setTemplateName( StormClusterConfiguration.TEMPLATE_NAME );
                nodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );
                nodeGroup.setNumberOfNodes( 1 );

                GsonBuilder builder = new GsonBuilder();
                Gson gson = builder.create();
                String ngJSON = gson.toJson(nodeGroup);

                trackerOperation.addLog( "Creating new containers..." );
                environmentManager.createAdditionalContainers( config.getEnvironmentId(), ngJSON, localPeer );
            }
            else{
                trackerOperation.addLog( "Using existing containers that are not taking role in cluster" );
            }


            // update cluster configuration on DB
            environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
            for ( ContainerHost containerHost : environmentManager.getEnvironmentByUUID( config.getEnvironmentId() ).getContainerHosts() )
            {
                if ( ! config.getAllNodes().contains( containerHost.getId() ) ){
                    config.getSupervisors().add( containerHost.getId() );
                    trackerOperation.addLog( containerHost.getHostname() + " is added as supervisor node." );
                }
            }
            manager.getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config );

            // configure ssh keys
            /*
                TODO: do we need to configure ssh keys of storm nodes?
                Set<ContainerHost> allNodes = new HashSet<>();
                allNodes.addAll( newlyCreatedContainers );
                allNodes.addAll( environment.getContainerHosts() );
                try
                {
                    manager.getSecurityManager().configHostsOnAgents( allNodes, Common.DEFAULT_DOMAIN_NAME );
                }
                catch ( SecurityManagerException e )
                {
                    e.printStackTrace();
                }

                // link hosts (/etc/hosts)
                try
                {
                    manager.getSecurityManager().configSshOnAgents( allNodes );
                }
                catch ( SecurityManagerException e )
                {
                    e.printStackTrace();
                }
            */

            // configure cluster
            try
            {
                configureCluster( config, environment );
            }
            catch ( ClusterSetupException e )
            {
                e.printStackTrace();
            }

            trackerOperation.addLogDone( "Finished." );
        }
        catch ( EnvironmentBuildException e )
        {
            e.printStackTrace();
        }
    }


    private void configureCluster( StormClusterConfiguration config, Environment environment )
            throws ClusterSetupException
    {
        trackerOperation.addLog( "Configuring cluster..." );
        try
        {
            new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, environment );
        }
        catch ( ClusterSetupException e )
        {
            e.printStackTrace();
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );

        }
    }


    private CommandResult executeCommand( ContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", command );
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void setupCluster()
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            trackerOperation.addLogFailed( "Malformed configuration" );
            return;
        }

        if ( manager.getCluster( clusterName ) != null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name '%s' already exists", clusterName ) );
            return;
        }

        try
        {
            Environment env = manager.getEnvironmentManager()
                                     .buildEnvironment( manager.getDefaultEnvironmentBlueprint( config ) );
            trackerOperation.addLog( String.format( "Environment created successfully", clusterName ) );

            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( EnvironmentBuildException | ClusterSetupException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup %s cluster %s : %s", config.getProductKey(), clusterName,
                            e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        StormClusterConfiguration config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        try
        {
            trackerOperation.addLog( "Destroying environment..." );
            manager.getEnvironmentManager().destroyEnvironment( config.getEnvironmentId() );
            if ( config.isExternalZookeeper() ) {
                ZookeeperClusterConfig zookeeperClusterConfig =
                        manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
                Environment zookeeperEnvironment =
                        manager.getEnvironmentManager().getEnvironmentByUUID(
                                zookeeperClusterConfig.getEnvironmentId() );
                ContainerHost nimbusNode = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
                nimbusNode.execute( new RequestBuilder( Commands.make( CommandType.PURGE ) ) );
            }
            manager.getPluginDAO().deleteInfo( config.getProductKey(), config.getClusterName() );
            trackerOperation.addLogDone( "Cluster destroyed" );
        }
        catch ( EnvironmentDestroyException e )
        {
            trackerOperation.addLogFailed( String.format( "Error destroying environment, %s", e.getMessage() ) );
            LOG.error( e.getMessage(), e );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Error uninstalling storm package, %s", e.getMessage() ) );
            LOG.error( e.getMessage(), e );
        }
    }


    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
            po.addLog( commandResult.getStdOut() );
        if ( po.getState() == OperationState.FAILED ) {
            po.addLogFailed( "" );
        }
        else {
            po.addLogDone( "" );
        }
    }
}
