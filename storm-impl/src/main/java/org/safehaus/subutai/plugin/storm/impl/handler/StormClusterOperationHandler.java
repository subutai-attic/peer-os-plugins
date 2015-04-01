package org.safehaus.subutai.plugin.storm.impl.handler;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
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
    CommandUtil commandUtil = new CommandUtil();

    public StormClusterOperationHandler( final StormImpl manager, final StormClusterConfiguration config,
                                         final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
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
                    environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException( String.format( "Couldn't find environment with id: %s",
                            config.getEnvironmentId().toString() ), e );
                    return;
                }
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.START, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.START, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.START, StormService.SUPERVISOR ) ) );
                    }
                }
                break;
            case STOP_ALL:
                try
                {
                    environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException( String.format( "Couldn't find environment by id: %s",
                            config.getEnvironmentId().toString() ), e );
                    return;
                }
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STOP, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STOP, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STOP, StormService.SUPERVISOR ) ) );
                    }
                }
                break;
            case STATUS_ALL:
                try
                {
                    environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException(
                            String.format( "Couldn't get environment by id: %s", config.getEnvironmentId().toString() ),
                            e );
                    return;
                }
                for ( ContainerHost containerHost : environment.getContainerHosts() )
                {
                    if ( config.getNimbus().equals( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STATUS, StormService.NIMBUS ) ) );
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STATUS, StormService.UI ) ) );
                    }
                    else if ( config.getSupervisors().contains( containerHost.getId() ) )
                    {
                        commandResultList.add( executeCommand( containerHost,
                                Commands.make( CommandType.STATUS, StormService.SUPERVISOR ) ) );
                    }
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
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( StormClusterConfiguration.PRODUCT_KEY, StormClusterConfiguration.TEMPLATE_NAME,
                1, 0, 0, new PlacementStrategy( "ROUND_ROBIN" ) );

        Topology topology = new Topology();

        topology.addNodeGroupPlacement( localPeer, nodeGroup );

        ContainerHost newNode;
        try
        {
            ContainerHost unUsedContainerInEnvironment = findUnUsedContainerInEnvironment( environmentManager );
            if( unUsedContainerInEnvironment != null )
            {
                newNode = unUsedContainerInEnvironment;
                config.getSupervisors().add( unUsedContainerInEnvironment.getId() );
            }
            else {
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

                newNode = newNodeSet.iterator().next();

                config.getSupervisors().add( newNode.getId() );
            }

            manager.saveConfig( config );

            // configure new supervisor node
            try
            {
                environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Couldn't find environment with id: %s",
                        config.getEnvironmentId().toString() ), e );
                return;
            }
            configureNStart( newNode, config, environment );

            trackerOperation.addLogDone( "Finished." );

            // subscribe to alerts
            try
            {
                manager.subscribeToAlerts( newNode );
            }
            catch ( MonitorException e )
            {
                assert newNode != null;
                LOG.error( newNode.getHostname() + " could not get subscribed to alerts.", e );
                e.printStackTrace();
            }
            trackerOperation.addLogDone( "Node added" );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "failed to add node:  %s", e ) );
        }
    }



    private ContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager )
    {
        ContainerHost unusedNode = null;

        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            Set<ContainerHost> containerHostSet = environment.getContainerHosts();
            for( ContainerHost host : containerHostSet )
            {
                if( (!config.getAllNodes().contains( host.getId())) && host.getTemplateName().equals( StormClusterConfiguration.TEMPLATE_NAME ) )
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


    private ContainerHost checkUnusedNode( ContainerHost node )
    {
        if( node != null)
        {
            for( StormClusterConfiguration config : manager.getClusters() )
            {
                if( !config.getAllNodes().contains( node.getId() ))
                {
                    return node;
                }
            }
        }
        return null;

    }


    private void configureNStart( ContainerHost stormNode, StormClusterConfiguration config, Environment environment )
    {

        String zk_servers = makeZookeeperServersList( config );
        ContainerHost nimbusHost = null;
        try
        {
            nimbusHost = environment.getContainerHostById( config.getNimbus() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Error getting container host by id: %s", config.getNimbus().toString() ), e );
            return;
        }
        Map<String, String> paramValues = new LinkedHashMap<>();
        paramValues.put( "storm.zookeeper.servers", zk_servers );
        paramValues.put( "storm.local.dir", "/var/lib/storm" );
        paramValues.put( "nimbus.host", nimbusHost.getIpByInterfaceName( "eth0" ) );
        for ( Map.Entry<String, String> entry : paramValues.entrySet() )
        {
            String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );
            try
            {
                CommandResult commandResult = stormNode.execute( new RequestBuilder( s ).withTimeout( 60 ) );
                trackerOperation.addLog(
                        String.format( "Storm %s%s configured for entry %s on %s", stormNode.getNodeGroupName(),
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
            ContainerHost node = environment.getContainerHostById( config.getNimbus() );
            RequestBuilder checkIfNimbusNodeRunning =  new RequestBuilder( Commands.make( CommandType.STATUS, StormService.NIMBUS ) );
            CommandResult result;
            try
            {
                result = commandUtil.execute( checkIfNimbusNodeRunning, node );
                if ( result.hasSucceeded() ){
                    if ( ! result.getStdOut().toLowerCase().contains( "not" ) ){
                        stormNode.execute( new RequestBuilder( Commands.make( CommandType.KILL, StormService.SUPERVISOR ) ) );
                        stormNode.execute( new RequestBuilder( Commands.make( CommandType.START, StormService.SUPERVISOR ) ) );
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
            String zk_name = config.getZookeeperClusterName();
            ZookeeperClusterConfig zk_config;
            zk_config = manager.getZookeeperManager().getCluster( zk_name );
            if ( zk_config != null )
            {
                StringBuilder sb = new StringBuilder();
                Environment zookeeperEnvironment = null;
                try
                {
                    zookeeperEnvironment =
                            manager.getEnvironmentManager().findEnvironment( zk_config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException( String.format( "Error environment not found with id: %s",
                            zk_config.getEnvironmentId().toString() ), e );
                    return "";
                }
                Set<ContainerHost> zookeeperNodes = null;
                try
                {
                    zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( zk_config.getNodes() );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    logException( String.format( "Some container hosts not found by ids: %s.",
                            zk_config.getNodes().toString() ), e );
                    return "";
                }
                for ( ContainerHost containerHost : zookeeperNodes )
                {
                    if ( sb.length() > 0 )
                    {
                        sb.append( "," );
                    }
                    sb.append( containerHost.getIpByInterfaceName( "eth0" ) );
                }
                return sb.toString();
            }
        }
        else if ( config.getNimbus() != null )
        {
            ContainerHost nimbusHost = null;
            try
            {
                nimbusHost = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                    .getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus().toString() ), e );
                return "";
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Environment not found by id: %s", config.getEnvironmentId().toString() ),
                        e );
                return "";
            }
            return nimbusHost.getIpByInterfaceName( "eth0" );
        }
        return null;
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
        // before removing cluster, stop it first.
        manager.stopAll( config.getClusterName() );

        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        if ( manager.getPluginDAO().deleteInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            trackerOperation.addLogDone( "Cluster information deleted from database" );
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
        }

        try
        {
            manager.unsubscribeFromAlerts( environment );
        }
        catch ( MonitorException e )
        {
            trackerOperation.addLog( String.format( "Failed to unsubscribe from alerts: %s", e.getMessage() ) );
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
