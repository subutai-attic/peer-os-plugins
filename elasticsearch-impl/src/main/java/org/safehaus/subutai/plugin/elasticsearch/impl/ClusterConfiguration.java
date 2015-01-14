package org.safehaus.subutai.plugin.elasticsearch.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
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
        Set<ContainerHost> esNodes = environment.getContainerHostsByIds( clusterConfiguration.getNodes() );

        String clusterConfigureCommand = Commands.configure + " cluster.name " + config.getClusterName();

        for ( ContainerHost containerHost : esNodes )
        {
            try
            {
                po.addLog( "Checking ES installation..." );
                CommandResult commandResult =
                        commandUtil.execute( new RequestBuilder( Commands.checkCommand ), containerHost );

                if ( !commandResult.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    //install ES on the node
                    po.addLog( String.format( "Installing ES on %s...", containerHost.getHostname() ) );
                    commandUtil
                            .execute( new RequestBuilder( Commands.installCommand ).withTimeout( 180 ), containerHost );
                }

                po.addLog( String.format( "Configuring node %s..." + containerHost.getHostname() ) );
                // Setting cluster name
                commandUtil.execute( new RequestBuilder( clusterConfigureCommand ), containerHost );
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
