package io.subutai.plugin.zookeeper.impl;


import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );

    private static final String DEFAULT_CONFIGURATION =
            "dataDir=/var/lib/zookeeper/data\n" + "clientPort=2181\n" + "tickTime=2000\n" + "initLimit=5\n"
                    + "syncLimit=2\n" + "#server.1=zookeeper1:2888:3888\n" + "#server.2=zookeeper2:2888:3888\n"
                    + "#server.3=zookeeper3:2888:3888";

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

        int nodeNumber = 0;
        List<CommandResult> commandsResultList = new ArrayList<>();

        for ( final EnvironmentContainerHost containerHost : containerHosts )
        {
            configureClusterCommand = Commands.getConfigureClusterCommand( prepareConfiguration( containerHosts ),
                    ConfigParams.CONFIG_FILE_PATH.getParamValue(), ++nodeNumber );
            try
            {
                CommandResult commandResult;
                commandResult =
                        containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                commandsResultList.add( commandResult );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not run command " + configureClusterCommand + ": " + e );
                LOG.error( "Could not run command " + configureClusterCommand + ": " + e );
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
            executeOnAllNodes( containerHosts,
                    new RequestBuilder( Commands.getStartZkServerCommand() ).withTimeout( 60 ) );
        }
        else
        {

            throw new ClusterConfigurationException( "Cluster configuration failed" );
        }
    }


    private void executeOnAllNodes( final Set<EnvironmentContainerHost> containerHosts, final RequestBuilder command )
            throws ClusterConfigurationException
    {
        po.addLog( "Cluster configured\nRestarting cluster..." );

        //restart all other nodes with new configuration

        removeSnaps( containerHosts );

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


    private void removeSnaps( final Set<EnvironmentContainerHost> containerHosts )
    {
        po.addLog( "Removing snaps..." );

        for ( final EnvironmentContainerHost containerHost : containerHosts )
        {
            try
            {
                containerHost.execute( new RequestBuilder( Commands.getRemoveSnapsCommand() ).withTimeout( 60 ) );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not remove snap in node:" + containerHost.getHostname() + ": " + e );
                LOG.error( "Could not remove snap in node:" + containerHost.getHostname() + ": " + e );
            }
        }
    }


    //temporary workaround until we get full configuration injection working
    private String prepareConfiguration( Set<EnvironmentContainerHost> nodes ) throws ClusterConfigurationException
    {
        String zooCfgFile = "";

        try
        {
            URL url = ZookeeperStandaloneSetupStrategy.class.getProtectionDomain().getCodeSource().getLocation();

            URLClassLoader loader =
                    new URLClassLoader( new URL[] { url }, Thread.currentThread().getContextClassLoader() );
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


    public void deleteConfiguration( final ZookeeperClusterConfig zookeeperClusterConfig,
                                     final Environment environment ) throws ClusterConfigurationException
    {
        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = environment.getContainerHostsByIds( zookeeperClusterConfig.getNodes() );

            for ( final EnvironmentContainerHost containerHost : containerHosts )
            {
                String configureClusterCommand = Commands.getResetClusterConfigurationCommand( DEFAULT_CONFIGURATION,
                        ConfigParams.CONFIG_FILE_PATH.getParamValue() );
                try
                {
                    containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                }
                catch ( CommandException e )
                {
                    po.addLogFailed( "Could not run command " + configureClusterCommand + ": " + e );
                    LOG.error( "Could not run command " + configureClusterCommand + ": " + e );
                }
            }

            removeSnaps( containerHosts );

            executeOnAllNodes( containerHosts, Commands.getKillZkServerCommand() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by ids" );
            LOG.error( "Error getting container hosts by ids", e );
        }
    }


    public void removeNode( final ContainerHost host )
    {
        String configureClusterCommand = Commands.getResetClusterConfigurationCommand( DEFAULT_CONFIGURATION,
                ConfigParams.CONFIG_FILE_PATH.getParamValue() );

        try
        {
            host.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
            host.execute( new RequestBuilder( Commands.getRemoveSnapsCommand() ).withTimeout( 60 ) );
            host.execute( Commands.getKillZkServerCommand() );
        }
        catch ( CommandException e )
        {
            po.addLogFailed( "Could not run command: " + e );
            LOG.error( "Could not run command: " + e );
        }
    }
}
