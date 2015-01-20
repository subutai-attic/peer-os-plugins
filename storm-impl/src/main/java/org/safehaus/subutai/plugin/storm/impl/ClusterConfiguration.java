package org.safehaus.subutai.plugin.storm.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


public class ClusterConfiguration
{
    private static final Logger LOG = Logger.getLogger( ClusterConfiguration.class.getName() );
    private TrackerOperation po;
    private StormImpl stormManager;
    private EnvironmentManager environmentManager;


    public ClusterConfiguration( final TrackerOperation operation, final StormImpl stormManager )
    {
        this.po = operation;
        this.stormManager = stormManager;
        environmentManager = stormManager.getEnvironmentManager();
    }


    public void configureCluster( StormClusterConfiguration config, Environment environment )
            throws ClusterConfigurationException, ClusterSetupException
    {
        String zk_servers = makeZookeeperServersList( config );
        if ( zk_servers == null )
        {
            throw new ClusterSetupException( "No Zookeeper instances" );
        }

        Map<String, String> paramValues = new LinkedHashMap<>();
        paramValues.put( "storm.zookeeper.servers", zk_servers );
        paramValues.put( "storm.local.dir", "/var/lib/storm" );

        ContainerHost nimbusHost;
        if ( config.isExternalZookeeper() ) {
            ZookeeperClusterConfig zookeeperClusterConfig =
                    stormManager.getZookeeperManager().getCluster( config.getZookeeperClusterName() );
            Environment zookeeperEnvironment =
                    environmentManager.getEnvironmentByUUID( zookeeperClusterConfig.getEnvironmentId() );
            nimbusHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
        }
        else {
            nimbusHost = environment.getContainerHostById( config.getNimbus() );
        }
        paramValues.put( "nimbus.host", nimbusHost.getIpByInterfaceName( "eth0" ) );

        Set<ContainerHost> supervisorNodes = environment.getContainerHostsByIds( config.getSupervisors() );

        Set<ContainerHost> allNodes = new HashSet<>(  );
        allNodes.add( nimbusHost );
        allNodes.addAll( supervisorNodes );

        ContainerHost stormNode;
        Iterator<ContainerHost> iterator = allNodes.iterator();

        while( iterator.hasNext() )
        {
            stormNode = iterator.next();
            int operation_count = 0;

            for ( Map.Entry<String, String> entry : paramValues.entrySet() )
            {
                String s = Commands.configure( "add", "storm.xml", entry.getKey(), entry.getValue() );


                // Install zookeeper on nimbus node if embedded zookeeper is selected
                if ( ! config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installZookeeperCommand = stormManager.getZookeeperManager().getCommand( org.safehaus.subutai
                            .plugin.zookeeper.api.CommandType.INSTALL );
                    CommandResult commandResult = null;
                    try
                    {
                        commandResult = stormNode.execute( new RequestBuilder( installZookeeperCommand ).withTimeout( 1800 ) );
                    }
                    catch ( CommandException e )
                    {
                        e.printStackTrace();
                    }
                    po.addLog( String.format( "Zookeeper %s installed on Storm nimbus node %s",
                            commandResult.hasSucceeded() ? "" : "not", stormNode.getHostname() ) );
                }
                // Install storm on zookeeper node if external zookeeper is selected
                else if ( config.isExternalZookeeper() && config.getNimbus().equals( stormNode.getId() )
                        && operation_count == 0 )
                {
                    String installStormCommand = Commands.make( org.safehaus.subutai.plugin.storm.impl.CommandType.INSTALL );
                    CommandResult commandResult = null;
                    try
                    {
                        commandResult = stormNode.execute( new RequestBuilder( installStormCommand ).withTimeout( 1800 ) );
                    }
                    catch ( CommandException e )
                    {
                        e.printStackTrace();
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
                    po.addLogFailed("Failed to configure " + stormNode + ": " + exception );
                    exception.printStackTrace();
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
                Environment zookeeperEnvironment = environmentManager.getEnvironmentByUUID( zk_config.getEnvironmentId() );
                Set<ContainerHost> zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( zk_config.getNodes() );
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
            ContainerHost nimbusHost = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() ).getContainerHostById( config.getNimbus() );
            return nimbusHost.getIpByInterfaceName( "eth0" );
        }
        return null;
    }
}
