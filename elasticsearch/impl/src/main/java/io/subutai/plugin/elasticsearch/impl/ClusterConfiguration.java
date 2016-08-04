package io.subutai.plugin.elasticsearch.impl;


import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private ElasticsearchImpl manager;
    private TrackerOperation po;
    CommandUtil commandUtil = new CommandUtil();


    public ClusterConfiguration( final ElasticsearchImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final ConfigBase config, Environment environment )
            throws ClusterConfigurationException
    {

        ElasticsearchClusterConfiguration clusterConfiguration = ( ElasticsearchClusterConfiguration ) config;
        Set<EnvironmentContainerHost> esNodes = null;
        try
        {
            esNodes = environment.getContainerHostsByIds( clusterConfiguration.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( CollectionUtil.isCollectionEmpty( esNodes ) )
        {
            throw new ClusterConfigurationException( "No nodes found in environment" );
        }

        StringBuilder sb = new StringBuilder();
        for ( EnvironmentContainerHost containerHost : esNodes )
        {
            sb.append( containerHost.getInterfaceByName( "eth0" ).getIp() ).append( "," );
        }
        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }
        String hosts = sb.toString();


        for ( EnvironmentContainerHost containerHost : esNodes )
        {
            try
            {
                po.addLog( String.format( "Configuring node %s...", containerHost.getHostname() ) );

                // Setting cluster name
                commandUtil
                        .execute( manager.getCommands().setClusterNameCommand( clusterConfiguration.getClusterName() ),
                                containerHost );

                // Setting node name
                commandUtil.execute( manager.getCommands().setNodeNameCommand( containerHost.getHostname() ),
                        containerHost );

                // Setting network host
                commandUtil.execute( manager.getCommands().setNetworkHostCommand(
                        containerHost.getInterfaceByName( "eth0" ).getIp() ), containerHost );

                // Setting unicast hosts
                commandUtil.execute( manager.getCommands().setUnicastHostsCommand( hosts ), containerHost );

                // Restart node
                commandUtil.execute( Commands.getRestartCommand(), containerHost );
            }
            catch ( CommandException e )
            {
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        po.addLog( "Saving cluster information..." );
        clusterConfiguration.setEnvironmentId( environment.getId() );
        try
        {
            manager.saveConfig( clusterConfiguration );
        }
        catch ( ClusterException e )
        {
            throw new ClusterConfigurationException( e );
        }
    }


    public void deleteClusterConfiguration( final ElasticsearchClusterConfiguration config,
                                            final Environment environment ) throws ClusterConfigurationException
    {
        for ( String id : config.getNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );

                // Stopping node
                CommandResult commandResult = containerHost.execute( Commands.getStopCommand() );
                po.addLog( commandResult.getStdOut() );

                String command = "sed -i \"s/.*discovery.zen.ping.unicast.hosts: .*/#discovery.zen.ping.unicast.hosts: "
                        + "[\"host1\", \"host2\"]/g\" " + "/etc/elasticsearch/elasticsearch" + ".yml";

                commandUtil.execute( new RequestBuilder( command ), containerHost );

                //                // Setting unicast hosts
                //                commandUtil.execute( manager.getCommands().setUnicastHostsCommand( "host1,host2" ),
                // containerHost );

                // Restart node
                commandUtil.execute( Commands.getRestartCommand(), containerHost );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Deleting configuration failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        try
        {
            manager.deleteConfig( config );
            po.addLogDone( "Cluster removed from database" );
        }
        catch ( ClusterException e )
        {
            po.addLogFailed( "Failed to delete cluster information from database" );
        }
    }
}
