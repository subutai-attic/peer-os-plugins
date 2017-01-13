package io.subutai.plugin.galera.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.plugin.galera.api.GaleraClusterConfig;


public class ClusterConfiguration
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );
    private GaleraImpl manager;
    private TrackerOperation po;


    public ClusterConfiguration( final GaleraImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final GaleraClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        Set<String> nodeUUIDs = config.getNodes();
        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = environment.getContainerHostsByIds( nodeUUIDs );
            String ips = collectHostnames( containerHosts );

            // stop mysql on all nodes
            executeOnAllNodes( containerHosts, Commands.getStopMySql() );


            ContainerHost initiator = containerHosts.iterator().next();
            configureInitiator( initiator, ips );
            config.setInitiator( initiator.getId() );
            containerHosts.remove( initiator );

            configureOtherNodes( containerHosts, ips );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by ids" );
            return;
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private void configureOtherNodes( final Set<EnvironmentContainerHost> containerHosts, final String ips )
            throws CommandException
    {
        for ( final EnvironmentContainerHost host : containerHosts )
        {
            host.execute( Commands.getSetClusterAddress( ips ) );
            host.execute( Commands.getSetNodeAddress( host.getIp() ) );
            host.execute( Commands.getStartMySql() );
        }
    }


    private void configureInitiator( final ContainerHost initiator, String ips ) throws CommandException
    {
        initiator.execute( Commands.getSetClusterAddress( ips ) );
        initiator.execute( Commands.getSetNodeAddress( initiator.getIp() ) );
        initiator.execute( Commands.getBootstrapMySql() );
    }


    public String collectHostnames( final Set<EnvironmentContainerHost> nodes )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost node : nodes )
        {
            sb.append( node.getIp() ).append( "," );
        }

        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }

        return sb.toString();
    }


    public void deleteConfiguration( final GaleraClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = environment.getContainerHostsByIds( config.getNodes() );
            cleanupConfiguration( containerHosts );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by ids" );
            LOG.error( "Error getting container hosts by ids", e );
        }
    }


    private void executeOnAllNodes( final Set<EnvironmentContainerHost> containerHosts, final RequestBuilder command )
            throws ClusterConfigurationException
    {
        for ( final EnvironmentContainerHost containerHost : containerHosts )
        {
            try
            {
                containerHost.execute( command );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not restart node:" + containerHost.getHostname() + ": " + e );
                LOG.error( "Could not restart node:" + containerHost.getHostname() + ": " + e );
            }
        }
    }


    public GaleraClusterConfig deleteNode( final Environment environment, final GaleraClusterConfig config,
                                           final ContainerHost host )
            throws ContainerHostNotFoundException, ClusterConfigurationException, CommandException
    {
        Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getNodes() );
        cleanupConfiguration( allNodes );

        allNodes.remove( host );

        return reconfigureCluster( allNodes, config );
    }


    public GaleraClusterConfig addNode( final Environment environment, final GaleraClusterConfig config,
                                        final EnvironmentContainerHost newNode )
            throws ContainerHostNotFoundException, ClusterConfigurationException, CommandException
    {
        Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getNodes() );
        cleanupConfiguration( allNodes );

        return reconfigureCluster( allNodes, config );
    }


    private GaleraClusterConfig reconfigureCluster( final Set<EnvironmentContainerHost> allNodes,
                                                    final GaleraClusterConfig config ) throws CommandException
    {
        // reconfiugre cluster
        String ips = collectHostnames( allNodes );

        ContainerHost initiator = allNodes.iterator().next();
        configureInitiator( initiator, ips );
        config.setInitiator( initiator.getId() );
        allNodes.remove( initiator );

        configureOtherNodes( allNodes, ips );

        return config;
    }


    private void cleanupConfiguration( final Set<EnvironmentContainerHost> allNodes )
            throws ClusterConfigurationException
    {
        // stop mariadb on all nodes
        executeOnAllNodes( allNodes, Commands.getStopMySql() );
        executeOnAllNodes( allNodes, Commands.getCleanupConfigCommand() );
    }
}
