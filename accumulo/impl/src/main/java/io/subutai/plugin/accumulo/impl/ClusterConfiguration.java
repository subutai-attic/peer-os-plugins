package io.subutai.plugin.accumulo.impl;


import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
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
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface<AccumuloClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );

    private final AccumuloImpl manager;
    private final TrackerOperation po;


    public ClusterConfiguration( final AccumuloImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    @Override
    public void configureCluster( final AccumuloClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        String configureClusterCommand;
        try
        {
            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );
            EnvironmentContainerHost namenode = environment.getContainerHostById( config.getMaster() );
            Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );

            int nodeNumber = 0;

            for ( final EnvironmentContainerHost containerHost : allNodes )
            {
                configureClusterCommand = Commands.getConfigureClusterCommand( prepareConfiguration( allNodes ),
                        ConfigParams.CONFIG_FILE_PATH.getParamValue(), ++nodeNumber );
                containerHost.execute( new RequestBuilder( Commands.CREATE_CONFIG_FILE ) );
                containerHost.execute( new RequestBuilder( configureClusterCommand ).withTimeout( 60 ) );
                containerHost.execute( Commands.getCopyConfigsCommand() );
                containerHost.execute( Commands.getRemoveConfigsCommand() );
                containerHost.execute( Commands.getAccumucoSiteConfig() );
                containerHost.execute( Commands.getSetMasterCommand( namenode.getHostname() ) );
                containerHost.execute( Commands.getSetSlavesCommand( collectHostnames( slaves ) ) );
                containerHost.execute( Commands.getSetInstanceVolume( namenode.getHostname() ) );
                containerHost.execute( Commands.getSetInstanceZkHost( collectHostnamesWithPorts( allNodes ) ) );
                containerHost.execute( Commands.getSetPassword( config.getPassword() ) );
                containerHost.execute( Commands.getSetJavaHeapSize() );
            }

            executeOnAllNodes( allNodes, Commands.getStartZkServerCommand() );
            namenode.execute( Commands.getInitializeCommand( config.getPassword(), config.getClusterName() ) );
            namenode.execute( Commands.getStartMasterCommand() );
            executeOnAllNodes( slaves, Commands.getStartSlaveCommand() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            po.addLogFailed( "Error getting container hosts by id" );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            po.addLogFailed( "Could not run command" + e.getMessage() );
            LOG.error( "Could not run command " + e.getMessage() );
            e.printStackTrace();
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
            URL url = AccumuloOverZkNHadoopSetup.class.getProtectionDomain().getCodeSource().getLocation();

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


    public String collectHostnamesWithPorts( final Set<EnvironmentContainerHost> nodes )
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


    public String collectHostnames( final Set<EnvironmentContainerHost> nodes )
    {
        StringBuilder sb = new StringBuilder();

        for ( final EnvironmentContainerHost node : nodes )
        {
            sb.append( node.getHostname() ).append( "\n" );
        }

        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }

        return sb.toString();
    }


    public void deleteNode( final EnvironmentContainerHost node ) throws CommandException, ClusterException
    {
        node.execute( Commands.getStopSlaveCommand() );
        CommandResult result = node.execute( Commands.getUninstallAccumuloCommand() );
        if ( !result.hasSucceeded() )
        {
            throw new ClusterException(
                    String.format( "Could not uninstall Accumulo from node %s : %s", node.getHostname(),
                            result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }


    public void reconfigureNodes( final AccumuloClusterConfig config, final Environment environment,
                                  final String hostname ) throws ContainerHostNotFoundException, CommandException
    {
        Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );

        for ( final EnvironmentContainerHost node : allNodes )
        {
            node.execute( Commands.getClearSlaveCommand( hostname ) );
        }
    }


    public void addNode( final AccumuloClusterConfig config, final Environment environment,
                         final EnvironmentContainerHost node ) throws CommandException, ContainerHostNotFoundException
    {
        Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );
        Set<EnvironmentContainerHost> slaves = environment.getContainerHostsByIds( config.getSlaves() );
        EnvironmentContainerHost namenode = environment.getContainerHostById( config.getMaster() );

        node.execute( Commands.getCopyConfigsCommand() );
        node.execute( Commands.getRemoveConfigsCommand() );
        node.execute( Commands.getAccumucoSiteConfig() );
        node.execute( Commands.getSetMasterCommand( namenode.getHostname() ) );

        for ( final EnvironmentContainerHost host : allNodes )
        {
            host.execute( Commands.getSetSlavesCommand( collectHostnames( slaves ) ) );
        }

        node.execute( Commands.getSetInstanceVolume( namenode.getHostname() ) );
        node.execute( Commands.getSetInstanceZkHost( collectHostnamesWithPorts( allNodes ) ) );
        node.execute( Commands.getSetPassword( config.getPassword() ) );
        node.execute( Commands.getSetJavaHeapSize() );
        node.execute( Commands.getStartZkServerCommand() );
        node.execute( Commands.getStartSlaveCommand() );
    }
}
