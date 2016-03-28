package io.subutai.plugin.storm.impl;


import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.Host;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface
{
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );
    private TrackerOperation po;
    private StormImpl stormManager;
    private EnvironmentManager environmentManager;
    private EnvironmentContainerHost nimbusHost;


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
            Environment zookeeperEnvironment;
            try
            {
                zookeeperEnvironment = environmentManager.loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                logException( String.format( "Environment not found by id: %s", config.getEnvironmentId() ), e );
                return;
            }
            try
            {
                nimbusHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                logException( String.format( "Container host not found by id: %s", config.getNimbus() ), e );
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
            try
			{
				stormNode.execute (new RequestBuilder ("apt-get --force-yes --assume-yes update").withTimeout (60));
				stormNode.execute (new RequestBuilder ("apt-get --force-yes --assume-yes install python").withTimeout (60));
			}
			catch (CommandException e)
			{
				e.printStackTrace ();
				return;
			}
            for ( Map.Entry<String, String> entry : paramValues.entrySet() )
            {
                String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );


                // Install zookeeper on nimbus node if embedded zookeeper is selected
                if ( !config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installZookeeperCommand = stormManager.getZookeeperManager().getCommand(
                            io.subutai.plugin.zookeeper.api.CommandType.INSTALL );
                    CommandResult commandResult;
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
                    String installStormCommand = Commands.make( CommandType.INSTALL );
                    CommandResult commandResult;
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

        /*try
        {
            stormManager.subscribeToAlerts( environment );
        }
        catch ( MonitorException e )
        {
            LOG.error( "Error while subscribing to alerts", e );
            e.printStackTrace();
        }*/
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
                Environment zookeeperEnvironment;
                try
                {
                    zookeeperEnvironment = environmentManager.loadEnvironment( zk_config.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    logException( String.format( "Environment not found by id: %s", zk_config.getEnvironmentId() ), e );
                    return "";
                }
                Set<EnvironmentContainerHost> zookeeperNodes;
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
                for ( EnvironmentContainerHost containerHost : zookeeperNodes )
                {
                    if ( sb.length() > 0 )
                    {
                        sb.append( "," );
                    }
                    HostInterface hostInterface = containerHost.getInterfaceByName ("eth0");
                    sb.append( hostInterface.getIp () );
                }
                return sb.toString();
            }
        }
        else if ( config.getNimbus() != null )
        {
            EnvironmentContainerHost nimbusHost;
            try
            {
                nimbusHost = environmentManager.loadEnvironment( config.getEnvironmentId() )
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
			HostInterface hostInterface = nimbusHost.getInterfaceByName ("eth0");
            return hostInterface.getIp ();
        }
        return null;
    }


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        po.addLogFailed( msg );
    }
}
