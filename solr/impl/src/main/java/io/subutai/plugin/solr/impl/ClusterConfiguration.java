package io.subutai.plugin.solr.impl;


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
import io.subutai.plugin.solr.api.SolrClusterConfig;


public class ClusterConfiguration
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );

    private static final String DEFAULT_CONFIGURATION =
            "dataDir=/var/lib/zookeeper/data\n" + "clientPort=2181\n" + "tickTime=2000\n" + "initLimit=5\n" + "syncLimit=2\n"
                    + "#server.1=zookeeper1:2888:3888\n" + "#server.2=zookeeper2:2888:3888\n"
                    + "#server.3=zookeeper3:2888:3888";

    private SolrImpl manager;
    private TrackerOperation po;


    public ClusterConfiguration( final SolrImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final SolrClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
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

        for ( final EnvironmentContainerHost containerHost : containerHosts )
        {
            configureClusterCommand = Commands.getConfigureClusterCommand( prepareConfiguration( containerHosts ),
                    ConfigParams.CONFIG_FILE_PATH.getParamValue(), ++nodeNumber );
            try
            {
                containerHost.execute( new RequestBuilder( Commands.CREATE_CONFIG_FILE ) );
                containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not run command " + configureClusterCommand + ": " + e );
                LOG.error( "Could not run command " + configureClusterCommand + ": " + e );
            }
        }

        executeOnAllNodes( containerHosts, Commands.getStartZkServerCommand() );
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
            URL url = SolrSetupStrategy.class.getProtectionDomain().getCodeSource().getLocation();

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


    private List<CommandResult> getFailedCommandResults( final List<CommandResult> commandResultList )
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


    public String collectHostnames( final Set<EnvironmentContainerHost> nodes )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost node : nodes )
        {
            sb.append( node.getHostname() ).append( ":2181" ).append( "," );
        }

        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }

        return sb.toString();
    }


    public void deleteConfiguration( final SolrClusterConfig solrClusterConfig, final Environment environment )
            throws ClusterConfigurationException
    {
        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = environment.getContainerHostsByIds( solrClusterConfig.getNodes() );

            for ( final EnvironmentContainerHost containerHost : containerHosts )
            {
                String configureClusterCommand = Commands.getResetClusterConfigurationCommand( DEFAULT_CONFIGURATION,
                        ConfigParams.CONFIG_FILE_PATH.getParamValue() );
                try
                {
                    containerHost.execute( Commands.getStopSolrCommand( collectHostnames( containerHosts ),
                            containerHost.getHostname() ) );
                    containerHost.execute( Commands.getKillZkServerCommand() );

                    containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                }
                catch ( CommandException e )
                {
                    po.addLogFailed( "Could not run command " + configureClusterCommand + ": " + e );
                    LOG.error( "Could not run command " + configureClusterCommand + ": " + e );
                }
            }

            removeSnaps( containerHosts );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by ids" );
            LOG.error( "Error getting container hosts by ids", e );
        }
    }
}
