package io.subutai.plugin.storm.impl.handler;


import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.NodeSchema;
import io.subutai.common.environment.Topology;
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.PeerException;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.strategy.api.ContainerPlacementStrategy;
import io.subutai.core.strategy.api.RoundRobinStrategy;
import io.subutai.core.strategy.api.StrategyException;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerSize;
import io.subutai.hub.share.resource.PeerGroupResources;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.ClusterConfiguration;
import io.subutai.plugin.storm.impl.CommandType;
import io.subutai.plugin.storm.impl.Commands;
import io.subutai.plugin.storm.impl.StormImpl;
import io.subutai.plugin.storm.impl.StormService;
//import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * This class handles operations that are related to whole cluster.
 */
public class StormClusterOperationHandler extends AbstractOperationHandler<StormImpl, StormClusterConfiguration>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( StormClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private StormClusterConfiguration config;
    private Environment environment;
    private TemplateManager templateManager;
    CommandUtil commandUtil = new CommandUtil();


    public StormClusterOperationHandler( final StormImpl manager, final TemplateManager templateManager,
                                         final StormClusterConfiguration config,
                                         final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        this.templateManager = templateManager;
        trackerOperation = manager.getTracker().createTrackerOperation( config.getProductKey(),
                String.format( "Running %s operation on %s...", operationType, clusterName ) );
    }


    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        runOperationOnContainers( operationType );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        List<CommandResult> commandResultList = new ArrayList<>();
        Environment zookeeperEnvironment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            if ( config.isExternalZookeeper() )
            {
                //                ZookeeperClusterConfig zookeeperConfig =
                //                        manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
                //                zookeeperEnvironment =
                //                        manager.getEnvironmentManager().loadEnvironment( zookeeperConfig
                // .getEnvironmentId() );
            }
            else
            {
                zookeeperEnvironment = environment;
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            logException( String.format( "Couldn't get environment by id: %s", config.getEnvironmentId() ), e );
            return;
        }

        switch ( clusterOperationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
                try
                {
                    for ( String uuid : config.getAllNodes() )
                    {
                        if ( config.getNimbus().equals( uuid ) )
                        {

                            //                            EnvironmentContainerHost containerHost =
                            // zookeeperEnvironment.getContainerHostById( uuid );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.START, StormService
                            // .NIMBUS ) ) );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.START, StormService.UI )
                            // ) );
                        }
                        else if ( config.getSupervisors().contains( uuid ) )
                        {
                            EnvironmentContainerHost containerHost = environment.getContainerHostById( uuid );
                            commandResultList.add( executeCommand( containerHost,
                                    Commands.make( CommandType.START, StormService.SUPERVISOR ) ) );
                        }
                    }
                }
                catch ( ContainerHostNotFoundException e )
                {
                    return;
                }
                break;
            case STOP_ALL:
                try
                {

                    for ( String uuid : config.getAllNodes() )
                    {
                        if ( config.getNimbus().equals( uuid ) )
                        {

                            //                            EnvironmentContainerHost containerHost =
                            // zookeeperEnvironment.getContainerHostById( uuid );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.STOP, StormService
                            // .NIMBUS ) ) );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.STOP, StormService.UI )
                            // ) );
                        }
                        else if ( config.getSupervisors().contains( uuid ) )
                        {
                            EnvironmentContainerHost containerHost = environment.getContainerHostById( uuid );
                            commandResultList.add( executeCommand( containerHost,
                                    Commands.make( CommandType.STOP, StormService.SUPERVISOR ) ) );
                        }
                    }
                }
                catch ( ContainerHostNotFoundException e )
                {
                    return;
                }

                break;
            case STATUS_ALL:
                try
                {
                    for ( String uuid : config.getAllNodes() )
                    {
                        if ( config.getNimbus().equals( uuid ) )
                        {

                            //                            EnvironmentContainerHost containerHost =
                            // zookeeperEnvironment.getContainerHostById( uuid );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.STATUS, StormService
                            // .NIMBUS ) ) );
                            //                            commandResultList.add( executeCommand( containerHost,
                            //                                    Commands.make( CommandType.STATUS, StormService.UI
                            // ) ) );
                        }
                        else if ( config.getSupervisors().contains( uuid ) )
                        {
                            EnvironmentContainerHost containerHost = environment.getContainerHostById( uuid );
                            commandResultList.add( executeCommand( containerHost,
                                    Commands.make( CommandType.STATUS, StormService.SUPERVISOR ) ) );
                        }
                    }
                }
                catch ( ContainerHostNotFoundException e )
                {
                    return;
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


    private void addNode()
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        EnvironmentContainerHost newNode;
        try
        {
            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );
            List<Integer> containersIndex = Lists.newArrayList();

            for ( final EnvironmentContainerHost containerHost : env.getContainerHosts() )
            {
                String numbers = containerHost.getContainerName().replace( "Container", "" ).trim();
                String contId = numbers.split( "-" )[0];
                containersIndex.add( Integer.parseInt( contId ) );
            }

            EnvironmentContainerHost unUsedContainerInEnvironment =
                    findUnUsedContainerInEnvironment( environmentManager );
            if ( unUsedContainerInEnvironment != null )
            {
                newNode = unUsedContainerInEnvironment;
                config.getSupervisors().add( unUsedContainerInEnvironment.getId() );
            }
            else
            {
                Set<EnvironmentContainerHost> newNodeSet = null;
                try
                {
                    String containerName = "Container" + String.valueOf( Collections.max( containersIndex ) + 1 );
                    NodeSchema node = new NodeSchema( containerName, new ContainerQuota( ContainerSize.LARGE ),
                            StormClusterConfiguration.TEMPLATE_NAME,
                            templateManager.getTemplateByName( StormClusterConfiguration.TEMPLATE_NAME ).getId() );
                    List<NodeSchema> nodes = new ArrayList<>();
                    nodes.add( node );

                    Blueprint blueprint = new Blueprint(
                            manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() ).getName(),
                            nodes );

                    ContainerPlacementStrategy strategy =
                            manager.getStrategyManager().findStrategyById( RoundRobinStrategy.ID );
                    PeerGroupResources peerGroupResources = manager.getPeerManager().getPeerGroupResources();
                    Map<ContainerSize, ContainerQuota> quotas = ContainerSize.getDefaultQuotas();

                    Topology topology =
                            strategy.distribute( blueprint.getName(), blueprint.getNodes(), peerGroupResources,
                                    quotas );

                    newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
                }
                catch ( EnvironmentNotFoundException | EnvironmentModificationException | StrategyException |
                        PeerException e )
                {
                    LOG.error( "Could not add new node(s) to environment." );
                    throw new ClusterException( e );
                }

                newNode = newNodeSet.iterator().next();
                config.getSupervisors().add( newNode.getId() );
            }

            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterConfiguration configuration = new ClusterConfiguration( trackerOperation, manager );
            configuration.deleteConfiguration( config, environment );

            configuration.configureCluster( config, environment );


            manager.saveConfig( config );

            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to find environment:  %s", e ) );
        }
        catch ( ClusterConfigurationException e )
        {
            e.printStackTrace();
        }
    }


    private EnvironmentContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager )
    {
        EnvironmentContainerHost unusedNode = null;

        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            Set<EnvironmentContainerHost> containerHostSet = environment.getContainerHosts();
            for ( EnvironmentContainerHost host : containerHostSet )
            {
                if ( ( !config.getAllNodes().contains( host.getId() ) ) && host.getTemplateName().equals(
                        StormClusterConfiguration.TEMPLATE_NAME ) )
                {
                    unusedNode = host;
                    break;
                }
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return checkUnusedNode( unusedNode );
    }


    private EnvironmentContainerHost checkUnusedNode( EnvironmentContainerHost node )
    {
        if ( node != null )
        {
            for ( StormClusterConfiguration config : manager.getClusters() )
            {
                if ( !config.getAllNodes().contains( node.getId() ) )
                {
                    return node;
                }
            }
        }
        return null;
    }


    private void configureNStart( EnvironmentContainerHost stormNode, StormClusterConfiguration config,
                                  Environment environment )
    {

        String zk_servers = makeZookeeperServersList( config );
        EnvironmentContainerHost nimbusHost;
        try
        {
            nimbusHost = environment.getContainerHostById( config.getNimbus() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by id: %s", config.getNimbus() ), e );
            return;
        }
        Map<String, String> paramValues = new LinkedHashMap<>();
        HostInterface hostInterface = nimbusHost.getInterfaceByName( "eth0" );
        paramValues.put( "storm.zookeeper.servers", zk_servers );
        paramValues.put( "storm.local.dir", "/var/lib/storm" );
        paramValues.put( "nimbus.host", hostInterface.getIp() );
        for ( Map.Entry<String, String> entry : paramValues.entrySet() )
        {
            String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );
            try
            {
                CommandResult commandResult = stormNode.execute( new RequestBuilder( s ).withTimeout( 60 ) );
                trackerOperation.addLog( String.format( "Storm %s configured for entry %s on %s",
                        commandResult.hasSucceeded() ? "" : " not", entry, stormNode.getHostname() ) );
            }
            catch ( CommandException exception )
            {

                logException( "Failed to configure " + stormNode, exception );
                return;
            }
        }

        // start supervisor node if cluster is running
        try
        {
            EnvironmentContainerHost node = environment.getContainerHostById( config.getNimbus() );
            RequestBuilder checkIfNimbusNodeRunning =
                    new RequestBuilder( Commands.make( CommandType.STATUS, StormService.NIMBUS ) );
            CommandResult result;
            try
            {
                result = commandUtil.execute( checkIfNimbusNodeRunning, node );
                if ( result.hasSucceeded() )
                {
                    if ( !result.getStdOut().toLowerCase().contains( "not" ) )
                    {
                        stormNode.execute(
                                new RequestBuilder( Commands.make( CommandType.KILL, StormService.SUPERVISOR ) ) );
                        stormNode.execute(
                                new RequestBuilder( Commands.make( CommandType.START, StormService.SUPERVISOR ) ) );
                    }
                }
            }
            catch ( CommandException e )
            {
                LOG.error( "Could not check if Storm Nimbus node is running" );
                e.printStackTrace();
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    private String makeZookeeperServersList( StormClusterConfiguration config )
    {
        if ( config.isExternalZookeeper() )
        {
            //            String zk_name = config.getZookeeperClusterName();
            //            ZookeeperClusterConfig zk_config;
            //            zk_config = manager.getZookeeperManager().getCluster( zk_name );
            //            if ( zk_config != null )
            //            {
            //                StringBuilder sb = new StringBuilder();
            //                Environment zookeeperEnvironment;
            //                try
            //                {
            //                    zookeeperEnvironment =
            //                            manager.getEnvironmentManager().loadEnvironment( zk_config.getEnvironmentId
            // () );
            //                }
            //                catch ( EnvironmentNotFoundException e )
            //                {
            //                    logException(
            //                            String.format( "Error environment not found with id: %s", zk_config
            // .getEnvironmentId() ),
            //                            e );
            //                    return "";
            //                }
            //                Set<EnvironmentContainerHost> zookeeperNodes;
            //                try
            //                {
            //                    zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( zk_config.getNodes() );
            //                }
            //                catch ( ContainerHostNotFoundException e )
            //                {
            //                    logException( String.format( "Some container hosts not found by ids: %s.",
            //                            zk_config.getNodes().toString() ), e );
            //                    return "";
            //                }
            //                for ( EnvironmentContainerHost containerHost : zookeeperNodes )
            //                {
            //                    if ( sb.length() > 0 )
            //                    {
            //                        sb.append( "," );
            //                    }
            //                    HostInterface hostInterface = containerHost.getInterfaceByName( "eth0" );
            //                    sb.append( hostInterface.getIp() );
            //                }
            //                return sb.toString();
            //            }
        }
        else if ( config.getNimbus() != null )
        {
            EnvironmentContainerHost nimbusHost;
            try
            {
                nimbusHost = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                    .getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
                return "";
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Environment not found by id: %s", config.getEnvironmentId() ), e );
                return "";
            }
            HostInterface hostInterface = nimbusHost.getInterfaceByName( "eth0" );
            return hostInterface.getIp();
        }
        return null;
    }


    private CommandResult executeCommand( EnvironmentContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            logException( "Could not execute command correctly: " + command, e );
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
            ClusterSetupStrategy clusterSetupStrategy = manager.getClusterSetupStrategy( config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( ClusterSetupException e )
        {
            logException( String.format( "Failed to setup %s cluster %s", config.getProductKey(), clusterName ), e );
        }
    }


    @Override
    public void destroyCluster()
    {
        StormClusterConfiguration config = manager.getCluster( clusterName );

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterConfiguration configuration = new ClusterConfiguration( trackerOperation, manager );
            configuration.deleteConfiguration( config, environment );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment", e );
            trackerOperation.addLogFailed( "Error getting environment" );
            e.printStackTrace();
        }
        catch ( ClusterConfigurationException e )
        {
            LOG.error( "Error in delete configuration step: ", e );
            trackerOperation.addLogFailed( "Error in delete configuration step" );
            e.printStackTrace();
        }

        if ( manager.getPluginDAO().deleteInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }
    }


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        trackerOperation.addLogFailed( msg );
    }


    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
        {
            po.addLog( commandResult.getStdOut() );
        }
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
        else
        {
            po.addLogDone( "" );
        }
    }
}
