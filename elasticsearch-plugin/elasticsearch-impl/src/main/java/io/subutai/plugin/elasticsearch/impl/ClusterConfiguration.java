package io.subutai.plugin.elasticsearch.impl;


import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
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


        for ( EnvironmentContainerHost containerHost : esNodes )
        {
            try
            {
                po.addLog( "Checking ES installation..." );
                CommandResult commandResult =
                        commandUtil.execute( manager.getCommands().getCheckInstallationCommand(), containerHost );

                if ( !commandResult.getStdOut().contains( ElasticsearchClusterConfiguration.PACKAGE_NAME ) )
                {
                    //install ES on the node
                    po.addLog( String.format( "Installing ES on %s...", containerHost.getHostname() ) );
                    commandUtil.execute( manager.getCommands().getInstallCommand(), containerHost );
                }

                po.addLog( String.format( "Configuring node %s...", containerHost.getHostname() ) );
                // Setting cluster name
                commandUtil.execute( manager.getCommands().getConfigureCommand( clusterConfiguration.getClusterName() ),
                        containerHost );
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
}
