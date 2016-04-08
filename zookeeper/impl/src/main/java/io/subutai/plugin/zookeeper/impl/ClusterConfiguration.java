package io.subutai.plugin.zookeeper.impl;


import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * Configures ZK cluster
 */
public class ClusterConfiguration
{

    private ZookeeperImpl manager;
    private TrackerOperation po;


    public ClusterConfiguration( final ZookeeperImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final ZookeeperClusterConfig config, Environment environment )
            throws ClusterConfigurationException
    {

        po.addLog( "Configuring cluster..." );


        String configureClusterCommand;
        Set<String> nodeUUIDs = config.getNodes();
        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = environment.getContainerHostsByIds( nodeUUIDs );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by ids" );
            return;
        }
        Iterator<EnvironmentContainerHost> iterator = containerHosts.iterator();

        int nodeNumber = 0;
        List<CommandResult> commandsResultList = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            configureClusterCommand = Commands.getConfigureClusterCommand( prepareConfiguration( containerHosts ),
                    ConfigParams.CONFIG_FILE_PATH.getParamValue(), ++nodeNumber );
            try
            {
                ContainerHost zookeeperNode = environment.getContainerHostById( iterator.next().getId() );
                CommandResult commandResult;
                commandResult =
                        zookeeperNode.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                commandsResultList.add( commandResult );
            }
            catch ( CommandException | ContainerHostNotFoundException e )
            {
                po.addLogFailed( "Could not run command " + configureClusterCommand + ": " + e );
                e.printStackTrace();
            }
        }

        boolean isSuccesful = true;
        for ( CommandResult aCommandsResultList : commandsResultList )
        {
            if ( !aCommandsResultList.hasSucceeded() )
            {
                isSuccesful = false;
            }
        }
        if ( isSuccesful )
        {
            po.addLog( "Cluster configured\nRestarting cluster..." );
            //restart all other nodes with new configuration
            commandsResultList = new ArrayList<>();
            while ( iterator.hasNext() )
            {
                String restartCommand = Commands.getRestartCommand();
                CommandResult commandResult = null;
                try
                {
                    ContainerHost zookeeperNode = environment.getContainerHostById( iterator.next().getId() );
                    commandResult = zookeeperNode.execute( new RequestBuilder( restartCommand ).withTimeout( 60 ) );
                }
                catch ( CommandException | ContainerHostNotFoundException e )
                {
                    po.addLogFailed( "Could not run command " + restartCommand + ": " + e );
                    e.printStackTrace();
                }
                commandsResultList.add( commandResult );
            }

            if ( getFailedCommandResults( commandsResultList ).size() == 0 )
            {
                po.addLog( "Cluster successfully restarted" );
            }
            else
            {
                po.addLog( String.format( "Failed to restart cluster, skipping..." ) );
            }
        }
        else
        {

            throw new ClusterConfigurationException( String.format( "Cluster configuration failed" ) );
        }
    }


    //temporary workaround until we get full configuration injection working
    private String prepareConfiguration( Set<EnvironmentContainerHost> nodes ) throws ClusterConfigurationException
    {
        String zooCfgFile = "";

        try
        {
            URL url = ZookeeperStandaloneSetupStrategy.class.getProtectionDomain().getCodeSource().getLocation();

            URLClassLoader loader = new URLClassLoader( new URL[] { url }, Thread.currentThread().getContextClassLoader() );
            InputStream is = loader.getResourceAsStream( "conf/zoo.cfg" );
            Scanner scanner = new Scanner( is ).useDelimiter( "\\A" );
            zooCfgFile = scanner.hasNext() ? scanner.next() : "";
            is.close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        if ( Strings.isNullOrEmpty( zooCfgFile ) )
        {
            throw new ClusterConfigurationException( "Zoo.cfg resource is missing" );
        }

        zooCfgFile = zooCfgFile
                .replace( "$" + ConfigParams.DATA_DIR.getPlaceHolder(), ConfigParams.DATA_DIR.getParamValue() );

        /*
        server.1=zookeeper1:2888:3888
        server.2=zookeeper2:2888:3888
        server.3=zookeeper3:2888:3888
         */

        StringBuilder serversBuilder = new StringBuilder();
        int id = 0;
        for ( ContainerHost agent : nodes )
        {
            serversBuilder.append( "server." ).append( ++id ).append( "=" ).append( agent.getHostname() )
                          .append( ConfigParams.PORTS.getParamValue() ).append( "\n" );
        }

        zooCfgFile = zooCfgFile.replace( "$" + ConfigParams.SERVERS.getPlaceHolder(), serversBuilder.toString() );


        return zooCfgFile;
    }


    public List<CommandResult> getFailedCommandResults( final List<CommandResult> commandResultList )
    {
        List<CommandResult> failedCommands = new ArrayList<>();
        for ( CommandResult commandResult : commandResultList )
        {
            if ( !commandResult.hasSucceeded() )
            {
                failedCommands.add( commandResult );
            }
        }
        return failedCommands;
    }
}
