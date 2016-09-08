package io.subutai.plugin.hive.impl;


import java.util.Set;

import io.subutai.common.host.HostInterface;

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
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hive.api.HiveConfig;


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

        try
        {
            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( hiveConfig.getAllNodes() );

            ContainerHost server = environment.getContainerHostById( hiveConfig.getServer() );
            ContainerHost namenode = environment.getContainerHostById( hiveConfig.getNamenode() );

            // configure hive server
            po.addLog( "Configuring server node: " + server.getHostname() );
            HostInterface hostInterface = server.getInterfaceByName( "eth0" );

            // copy derby jars to hive/lib
            executeCommand( server, Commands.copyDerbyJarsCommand );

            // edit hive-site.xml
            executeCommand( server, Commands.configureHiveServer( hostInterface.getIp() ) );

            // create HDFS directories
            executeCommand( namenode, Commands.CREATE_HDFS_DIRECTORIES );

            // stop namenode
            executeCommand( namenode, Commands.STOP_NAMENODE_COMMAND );

            // add authentication step to core-site.xml
            executeCommand( namenode, Commands.addConfigHostsCoreSite() );
            executeCommand( namenode, Commands.addConfigGroupsCoreSite() );
            executeCommand( namenode, "sed -i -e \"s/bin/*/g\" "
                    + "/opt/hadoop/etc/hadoop/core-site.xml" );

            // start namenode
            executeCommand( namenode, Commands.START_NAMENODE_COMMAND );

            // start derby
            executeCommand( server, Commands.START_DERBY_COMMAND );

            // initialize schema
            executeCommand( server, Commands.INITIALIZE_SCHEMA );

            // start hiveserver2
            executeCommand( server, Commands.START_COMMAND );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
            LOGGER.error( "Error getting server container host.", e );
            po.addLogFailed( "Error getting server container host." );
            return;
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
