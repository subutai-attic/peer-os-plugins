package org.safehaus.subutai.plugin.elasticsearch.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


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
        Set<ContainerHost> esNodes = null;
        try
        {
            esNodes = environment.getContainerHostsByIds( clusterConfiguration.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }


        for ( ContainerHost containerHost : esNodes )
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

                po.addLog( String.format( "Configuring node %s..." + containerHost.getHostname() ) );
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
