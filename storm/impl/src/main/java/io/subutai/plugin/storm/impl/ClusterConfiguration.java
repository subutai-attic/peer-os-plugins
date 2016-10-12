package io.subutai.plugin.storm.impl;


import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import io.subutai.common.host.HostInterface;

import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.storm.api.StormClusterConfiguration;


public class ClusterConfiguration implements ClusterConfigurationInterface
{
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );

    private static final String DEFAULT_CONFIGURATION =
            "dataDir=/var/zookeeper\n" + "clientPort=2181\n" + "tickTime=2000\n" + "initLimit=5\n" + "syncLimit=2\n"
                    + "#server.1=zookeeper1:2888:3888\n" + "#server.2=zookeeper2:2888:3888\n"
                    + "#server.3=zookeeper3:2888:3888";

    private TrackerOperation po;
    private StormImpl stormManager;
    private EnvironmentManager environmentManager;
    private EnvironmentContainerHost nimbusHost;


    public ClusterConfiguration( final TrackerOperation operation, final StormImpl stormManager )
    {
        this.po = operation;
        this.stormManager = stormManager;
        environmentManager = stormManager.getEnvironmentManager();
    }


    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        StormClusterConfiguration config = ( StormClusterConfiguration ) configBase;
        String configureClusterCommand;

        try
        {
            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );
            Set<EnvironmentContainerHost> supervisorNodes =
                    environment.getContainerHostsByIds( config.getSupervisors() );
            Set<EnvironmentContainerHost> nimbusSet = new HashSet<>();
            EnvironmentContainerHost nimbus = environment.getContainerHostById( config.getNimbus() );
            nimbusSet.add( nimbus );

            String nimbusIp = collectIp( nimbusSet );
            String allIps = collectIp( allNodes );

            int nodeNumber = 0;

            for ( final EnvironmentContainerHost containerHost : allNodes )
            {
                configureClusterCommand = Commands.getConfigureClusterCommand( prepareConfiguration( allNodes ),
                        ConfigParams.CONFIG_FILE_PATH.getParamValue(), ++nodeNumber );
                containerHost.execute( new RequestBuilder( Commands.CREATE_CONFIG_FILE ) );
                containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                containerHost.execute( Commands.getCreateDataDirectoryCommand() );
                containerHost.execute( Commands.getConfigureZookeeperServersCommand( allIps ) );
                containerHost.execute( Commands.getConfigureNimbusSeedsCommand( nimbusIp ) );
            }

            nimbus.execute( Commands.getStartNimbusCommand() );
            nimbus.execute( Commands.getStartStormUICommand() );

            executeOnAllNodes( supervisorNodes, Commands.getStartSupervisorCommand() );
            executeOnAllNodes( allNodes, Commands.getStartZkServerCommand() );


        }
        catch ( ContainerHostNotFoundException | CommandException e )
        {
            logException( e.getMessage(), e );
            e.printStackTrace();
        }


        config.setEnvironmentId( environment.getId() );
        stormManager.getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_NAME, config.getClusterName(), config );
        po.addLogDone( "Cluster info successfully saved" );
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
                po.addLogFailed( "Could not start node:" + containerHost.getHostname() + ": " + e );
                LOG.error( "Could not restart node:" + containerHost.getHostname() + ": " + e );
            }
        }
    }


    public String collectIp( final Set<EnvironmentContainerHost> nodes )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost node : nodes )
        {
            sb.append( "'\\\\n'\\" ).append( "  " ).append( "-" ).append( " " ).append( "\"" )
              .append( node.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp() ).append( "\"" );
        }

        return sb.toString();
    }


    private void removeSnaps( final Set<EnvironmentContainerHost> containerHosts )
    {
        po.addLog( "Removing snaps..." );

        for ( final EnvironmentContainerHost containerHost : containerHosts )
        {
            try
            {
                containerHost.execute( new RequestBuilder( Commands.REMOVE_SNAPS_COMMAND ).withTimeout( 60 ) );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not remove snap in node:" + containerHost.getHostname() + ": " + e );
                LOG.error( "Could not remove snap in node:" + containerHost.getHostname() + ": " + e );
            }
        }
    }


    private String prepareConfiguration( Set<EnvironmentContainerHost> nodes ) throws ClusterConfigurationException
    {
        String zooCfgFile = "";

        try
        {
            URL url = StormSetupStrategyDefault.class.getProtectionDomain().getCodeSource().getLocation();

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


    private void logException( String msg, Exception e )
    {
        LOG.error( msg, e );
        po.addLogFailed( msg );
    }
}
