package io.subutai.plugin.hive.impl;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.hive.api.HiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ClusterConfiguration.class );
    private HiveImpl manager;
    private TrackerOperation po;


    public ClusterConfiguration( final HiveImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final ConfigBase config, Environment environment )
            throws ClusterConfigurationException
    {
        HiveConfig hiveConfig = ( HiveConfig ) config;
        ContainerHost server = null;
        try
        {
            server = environment.getContainerHostById( ( ( HiveConfig ) config ).getServer() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Error getting server container host.", e );
            po.addLogFailed( "Error getting server container host." );
            return;
        }

        // configure hive server
        po.addLog( "Configuring server node: " + server.getHostname() );
        executeCommand( server, Commands.configureHiveServer( server.getIpByInterfaceName( "eth0" ) ) );


        for ( ContainerHost containerHost : environment.getContainerHosts() )
        {
            if ( !containerHost.getId().equals( server.getId() ) )
            {
                po.addLog( "Configuring client node : " + containerHost.getHostname() );
                executeCommand( containerHost, Commands.configureClient( server ) );
            }
        }
        hiveConfig.setEnvironmentId( environment.getId() );
        manager.getPluginDAO().saveInfo( HiveConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLogDone( HiveConfig.PRODUCT_KEY + " cluster data saved into database" );
    }


    public void executeCommand( ContainerHost host, String command )
    {
        try
        {
            host.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }
}
