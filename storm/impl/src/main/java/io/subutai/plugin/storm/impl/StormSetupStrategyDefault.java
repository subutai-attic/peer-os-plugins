package io.subutai.plugin.storm.impl;


import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.subutai.common.host.HostInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
//import io.subutai.plugin.zookeeper.api.CommandType;
//import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


public class StormSetupStrategyDefault implements ClusterSetupStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger( StormSetupStrategyDefault.class );
    private final StormImpl manager;
    private final StormClusterConfiguration config;
    private final TrackerOperation po;
    private Environment environment;
    private EnvironmentContainerHost nimbusHost;


    public StormSetupStrategyDefault( StormImpl manager, StormClusterConfiguration config, TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.po = po;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
//        ZookeeperClusterConfig zookeeperClusterConfig =
//                manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
//
//        try
//        {
//            environment = manager.getEnvironmentManager().loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
//        }
//        catch ( EnvironmentNotFoundException e )
//        {
//            logException( String.format( "Environment not found by id: %s", zookeeperClusterConfig.getEnvironmentId() ),
//                    e );
//            return null;
//        }
//        if ( environment == null )
//        {
//            throw new ClusterSetupException( "Environment not specified" );
//        }
//
//        if ( environment.getContainerHosts() == null || environment.getContainerHosts().isEmpty() )
//        {
//            throw new ClusterSetupException( "Environment has no nodes" );
//        }

        // check installed packages
//        for ( EnvironmentContainerHost n : environment.getContainerHosts() )
//        {
//            try
//            {
//                if ( !n.getTemplate().getProducts().contains( Commands.PACKAGE_NAME ) )
//                {
//                    throw new ClusterSetupException(
//                            String.format( "Node %s does not have Storm installed", n.getHostname() ) );
//                }
//            }
//            catch ( PeerException e )
//            {
//                logException( String.format( "Couldn't get container template" ), e );
//                return null;
//            }
//        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException( String.format( "Cluster '%s' already exists", config.getClusterName() ) );
        }

        if ( config.isExternalZookeeper() )
        {
//            if ( config.getNimbus() == null )
//            {
//                throw new ClusterSetupException( "Nimbus node not specified" );
//            }
//
//            String n = config.getZookeeperClusterName();
//            ZookeeperClusterConfig zk = manager.getZookeeperManager().getCluster( n );
//            if ( zk == null )
//            {
//                throw new ClusterSetupException( "Zookeeper cluster not found: " + config.getZookeeperClusterName() );
//            }
//
//            if ( !zk.getNodes().contains( config.getNimbus() ) )
//            {
//                throw new ClusterSetupException(
//                        "Specified nimbus node is not part of Zookeeper cluster " + config.getZookeeperClusterName() );
//            }
        }
        else
        // find out nimbus node in environment
        {
            for ( EnvironmentContainerHost n : environment.getContainerHosts() )
            {
                if ( n.getNodeGroupName().equals( StormService.NIMBUS.toString() ) )
                {
                    config.setNimbus( n.getId() );
                }
            }
        }

        // collect worker nodes in environment
        for ( EnvironmentContainerHost n : environment.getContainerHosts() )
        {
            if ( n.getNodeGroupName().equals( StormService.SUPERVISOR.toString() ) )
            {
                config.getSupervisors().add( n.getId() );
            }
        }
        if ( config.getNimbus() == null )
        {
            throw new ClusterSetupException( "Environment has no Nimbus node" );
        }
        if ( config.getSupervisorsCount() != config.getSupervisors().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Environment has %d nodes instead of %d", config.getSupervisors().size(),
                            config.getSupervisorsCount() ) );
        }

        try
        {
            environment.getContainerHostById( config.getNimbus() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
            return null;
        }

        //TODO enable these checks when isConnected method working OK
        //        if ( !containerHost.isConnected() )
        //        {
        //            throw new ClusterSetupException( "Nimbus node is not connected" );
        //        }
        //        for ( UUID supervisorUuids : config.getSupervisors() )
        //        {
        //            if ( ! environment
        //                    .getContainerHostById( supervisorUuids ).isConnected() )
        //            {
        //                throw new ClusterSetupException( "Not all worker nodes are connected" );
        //            }
        //        }

        configure();

        config.setEnvironmentId( environment.getId() );
        manager.getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_NAME, config.getClusterName(), config );
        po.addLog( "Cluster info successfully saved" );

        return config;
    }


    public void configure() throws ClusterSetupException
    {
        String zk_servers = makeZookeeperServersList( config );
        if ( zk_servers == null )
        {
            throw new ClusterSetupException( "No Zookeeper instances" );
        }

        Map<String, String> paramValues = new LinkedHashMap<>();
        paramValues.put( "storm.zookeeper.servers", zk_servers );
        paramValues.put( "storm.local.dir", "/var/lib/storm" );

        if ( config.isExternalZookeeper() )
        {
//            ZookeeperClusterConfig zookeeperClusterConfig =
//                    manager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
//            Environment zookeeperEnvironment;
//            try
//            {
//                zookeeperEnvironment =
//                        manager.getEnvironmentManager().loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
//            }
//            catch ( EnvironmentNotFoundException e )
//            {
//                logException(
//                        String.format( "Environment not found by id: %s", zookeeperClusterConfig.getEnvironmentId() ),
//                        e );
//                return;
//            }
//            try
//            {
//                nimbusHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
//            }
//            catch ( ContainerHostNotFoundException e )
//            {
//                logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
//                return;
//            }
        }
        else
        {
            try
            {
                nimbusHost = environment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
                return;
            }
        }
		HostInterface hostInterface = nimbusHost.getInterfaceByName ("eth0");
        paramValues.put( "nimbus.host", hostInterface.getIp () );

        Set<EnvironmentContainerHost> supervisorNodes;
        try
        {
            supervisorNodes = environment.getContainerHostsByIds( config.getSupervisors() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logException( String.format( "Container host not found by id: %s", config.getSupervisors().toString() ),
                    e );
            return;
        }

        Set<EnvironmentContainerHost> allNodes = new HashSet<>();
        allNodes.add( nimbusHost );
        allNodes.addAll( supervisorNodes );

        EnvironmentContainerHost stormNode;

        for ( final EnvironmentContainerHost allNode : allNodes )
        {
            stormNode = allNode;
            int operation_count = 0;

            for ( Map.Entry<String, String> entry : paramValues.entrySet() )
            {
                String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );


                // Install zookeeper on nimbus node if embedded zookeeper is selected
                if ( !config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
//                    String installZookeeperCommand = manager.getZookeeperManager().getCommand( CommandType.INSTALL );
//                    CommandResult commandResult;
//                    try
//                    {
//                        commandResult =
//                                stormNode.execute( new RequestBuilder( installZookeeperCommand ).withTimeout( 1800 ) );
//                    }
//                    catch ( CommandException e )
//                    {
//                        logException( "Error executing command " + installZookeeperCommand, e );
//                        return;
//                    }
//                    po.addLog( String.format( "Zookeeper %s installed on Storm nimbus node %s",
//                            commandResult.hasSucceeded() ? "" : "not", stormNode.getHostname() ) );
                }
                // Install storm on zookeeper node if external zookeeper is selected
                else if ( config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installStormCommand = Commands.make( io.subutai.plugin.storm.impl.CommandType.INSTALL );
                    CommandResult commandResult;
                    try
                    {
                        commandResult =
                                stormNode.execute( new RequestBuilder( installStormCommand ).withTimeout( 1800 ) );
                    }
                    catch ( CommandException e )
                    {
                        logException( "Error executing command " + installStormCommand, e );
                        return;
                    }
                    po.addLog( String.format( "Storm %s installed on zookeeper node %s",
                            commandResult.hasSucceeded() ? "" : "not", stormNode.getHostname() ) );
                }
                try
                {
                    CommandResult commandResult = stormNode.execute( new RequestBuilder( s ).withTimeout( 60 ) );
                    po.addLog( String.format( "Storm %s%s configured for entry %s on %s", stormNode.getNodeGroupName(),
                            commandResult.hasSucceeded() ? "" : " not", entry, stormNode.getHostname() ) );
                }
                catch ( CommandException exception )
                {
                    logException( "Failed to configure " + stormNode, exception );
                }
                operation_count++;
            }
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
//                            manager.getEnvironmentManager().loadEnvironment( zk_config.getEnvironmentId() );
//                }
//                catch ( EnvironmentNotFoundException e )
//                {
//                    logException( String.format( "Environment not found by id: %s", zk_config.getEnvironmentId() ), e );
//                    return "";
//                }
//                Set<EnvironmentContainerHost> zookeeperNodes;
//                try
//                {
//                    zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( zk_config.getNodes() );
//                }
//                catch ( ContainerHostNotFoundException e )
//                {
//                    logException( String.format( "Some container hosts not found by id: %s",
//                            zk_config.getNodes().toString() ), e );
//                    return "";
//                }
//                for ( EnvironmentContainerHost containerHost : zookeeperNodes )
//                {
//                    if ( sb.length() > 0 )
//                    {
//                        sb.append( "," );
//                    }
//					HostInterface hostInterface = containerHost.getInterfaceByName ("eth0");
//                    sb.append( hostInterface.getIp () );
//                }
//                return sb.toString();
//            }
        }
        else if ( config.getNimbus() != null )
        {
            EnvironmentContainerHost nimbusHost;
            try
            {
                nimbusHost = environment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
                return "";
            }
            HostInterface hostInterface = nimbusHost.getInterfaceByName ("eth0");
            return hostInterface.getIp ();
        }

        return null;
    }


    private void logException( String msg, Exception e )
    {
        LOGGER.error( msg, e );
        po.addLogFailed( msg );
    }
}
