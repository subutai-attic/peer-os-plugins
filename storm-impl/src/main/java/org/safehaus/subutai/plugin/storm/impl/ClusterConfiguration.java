package org.safehaus.subutai.plugin.storm.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.LoggerFactory;


public class ClusterConfiguration implements ClusterConfigurationInterface
{
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );
    private TrackerOperation po;
    private StormImpl stormManager;
    private EnvironmentManager environmentManager;
    private ContainerHost nimbusHost;


    public ClusterConfiguration( final TrackerOperation operation, final StormImpl stormManager )
    {
        this.po = operation;
        this.stormManager = stormManager;
        environmentManager = stormManager.getEnvironmentManager();
    }

    /**
     * TODO: configuration of External Installation should modify, especially configuration of nimbus on zookeeper node
     */

    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        StormClusterConfiguration config = ( StormClusterConfiguration ) configBase;
        String zk_servers = makeZookeeperServersList( config );
        if ( zk_servers == null )
        {
            throw new ClusterConfigurationException( "No Zookeeper instances" );
        }

        Map<String, String> paramValues = new LinkedHashMap<>();
        paramValues.put( "storm.zookeeper.servers", zk_servers );
        paramValues.put( "storm.local.dir", "/var/lib/storm" );

        if ( config.isExternalZookeeper() )
        {
            ZookeeperClusterConfig zookeeperClusterConfig =
                    stormManager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
            Environment zookeeperEnvironment = null;
            try
            {
                zookeeperEnvironment = environmentManager.findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Environment not found by id: %s", config.getEnvironmentId().toString() ),
                        e );
                return;
            }
            try
            {
                nimbusHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus().toString() ), e );
                return;
            }
        }
        else
        {
            try
            {
                nimbusHost = environment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus().toString() ), e );
                return;
            }
        }
        paramValues.put( "nimbus.host", nimbusHost.getIpByInterfaceName( "eth0" ) );

        Set<ContainerHost> supervisorNodes = null;
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

        Set<ContainerHost> allNodes = new HashSet<>();
        allNodes.add( nimbusHost );
        allNodes.addAll( supervisorNodes );

        ContainerHost stormNode;

        for ( Iterator<ContainerHost> iterator = allNodes.iterator(); iterator.hasNext(); )
        {
            stormNode = iterator.next();
            int operation_count = 0;

            for ( Map.Entry<String, String> entry : paramValues.entrySet() )
            {
                String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );


                // Install zookeeper on nimbus node if embedded zookeeper is selected
                if ( !config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installZookeeperCommand = stormManager.getZookeeperManager().getCommand(
                            org.safehaus.subutai.plugin.zookeeper.api.CommandType.INSTALL );
                    CommandResult commandResult = null;
                    try
                    {
                        commandResult =
                                stormNode.execute( new RequestBuilder( installZookeeperCommand ).withTimeout( 1800 ) );
                    }
                    catch ( CommandException e )
                    {
                        logException( String.format( "Error executing command: %s", installZookeeperCommand ), e );
                        return;
                    }
                    po.addLog( String.format( "Zookeeper %s installed on Storm nimbus node %s",
                            commandResult.hasSucceeded() ? "" : "not", stormNode.getHostname() ) );
                }
                // Install storm on zookeeper node if external zookeeper is selected
                else if ( config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installStormCommand =
                            Commands.make( org.safehaus.subutai.plugin.storm.impl.CommandType.INSTALL );
                    CommandResult commandResult = null;
                    try
                    {
                        commandResult =
                                stormNode.execute( new RequestBuilder( installStormCommand ).withTimeout( 1800 ) );
                    }
                    catch ( CommandException e )
                    {
                        logException( String.format( "Error executing command: %s", installStormCommand ), e );
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
        config.setEnvironmentId( environment.getId() );
        stormManager.getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_NAME, config.getClusterName(), config );
        po.addLogDone( "Cluster info successfully saved" );
    }


    private String makeZookeeperServersList( StormClusterConfiguration config )
    {
        if ( config.isExternalZookeeper() )
        {
            String zk_name = config.getZookeeperClusterName();
            ZookeeperClusterConfig zk_config;
            zk_config = stormManager.getZookeeperManager().getCluster( zk_name );
            if ( zk_config != null )
            {
                StringBuilder sb = new StringBuilder();
                Environment zookeeperEnvironment = null;
                try
                {
                    zookeeperEnvironment = environmentManager.findEnvironment( zk_config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException(
                            String.format( "Environment not found by id: %s", zk_config.getEnvironmentId().toString() ),
                            e );
                    return "";
                }
                Set<ContainerHost> zookeeperNodes = null;
                try
                {
                    zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( zk_config.getNodes() );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    logException( String.format( "Some container hosts not found by id: %s",
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
                nimbusHost = environmentManager.findEnvironment( config.getEnvironmentId() )
                                               .getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus().toString() ), e );
                return "";
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException(
                        String.format( "Environment not found by id: %s", config.getEnvironmentId().toString() ),
                        e );
                return "";
            }
            return nimbusHost.getIpByInterfaceName( "eth0" );
        }
        return null;
    }


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        po.addLogFailed( msg );
    }
}
