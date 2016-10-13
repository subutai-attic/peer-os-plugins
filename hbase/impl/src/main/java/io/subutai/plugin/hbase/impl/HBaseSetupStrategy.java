package io.subutai.plugin.hbase.impl;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.impl.handler.ClusterOperationHandler;


public class HBaseSetupStrategy
{

    private static final Logger LOG = LoggerFactory.getLogger( HBaseSetupStrategy.class.getName() );
    private Hadoop hadoop;
    private HBaseImpl manager;
    private HBaseConfig config;
    private Environment environment;
    private TrackerOperation trackerOperation;
    private CommandUtil commandUtil;
    private EnvironmentContainerHost master;
    private Set<EnvironmentContainerHost> regions;


    public HBaseSetupStrategy( HBaseImpl manager, Hadoop hadoop, HBaseConfig config, Environment environment,
                               TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.environment = environment;
        this.trackerOperation = po;
        this.hadoop = hadoop;
        this.commandUtil = new CommandUtil();
    }


    public ConfigBase setup() throws ClusterSetupException
    {
        checkConfig();

        Set<EnvironmentContainerHost> nodes = new HashSet<>();
        try
        {
            nodes = environment.getContainerHostsByIds( config.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( nodes.size() < config.getAllNodes().size() )
        {
            throw new ClusterSetupException( "Fewer nodes found in the environment than expected" );
        }
        for ( EnvironmentContainerHost node : nodes )
        {
            if ( !node.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }
        }

        if ( Strings.isNullOrEmpty( config.getClusterName() ) || CollectionUtil
                .isCollectionEmpty( config.getAllNodes() ) )
        {
            throw new ClusterSetupException( "Malformed configuration\nInstallation aborted" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists\nInstallation aborted",
                            config.getClusterName() ) );
        }

        if ( config.getAllNodes().isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
        }

        // Check installed packages
        trackerOperation.addLog( "Checking prerequisites..." );

        try
        {
            master = environment.getContainerHostById( config.getHbaseMaster() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found" + config.getHbaseMaster(), e );
            trackerOperation.addLogFailed( "Container host not found" );
        }

        try
        {
            regions = environment.getContainerHostsByIds( config.getRegionServers() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found" + config.getRegionServers(), e );
            trackerOperation.addLogFailed( "Containers host not found" );
        }


        trackerOperation.addLog( "Installing HBaseMaster..." );

        try
        {
            master.execute( new RequestBuilder( Commands.UPDATE_COMAND ).withTimeout( 20000 ).withStdOutRedirection(
                    OutputRedirection.NO ) );
            if ( !checkIfProductIsInstalled( master, Commands.PACKAGE_NAME ) )
            {
                CommandResult result = master.execute(
                        new RequestBuilder( Commands.INSTALL_COMMAND ).withTimeout( 20000 )
                                                                      .withStdOutRedirection( OutputRedirection.NO ) );
                checkInstalled( master, result );
            }
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( "Failed to install hbase on master node !!! " );
        }

        trackerOperation.addLog( "Master installation completed" );

        // installation of regions
        trackerOperation.addLog( "Installing regions..." );
        for ( EnvironmentContainerHost client : regions )
        {
            try
            {
                client.execute( new RequestBuilder( Commands.UPDATE_COMAND ).withTimeout( 20000 ).withStdOutRedirection(
                        OutputRedirection.NO ) );
                if ( !checkIfProductIsInstalled( client, Commands.PACKAGE_NAME ) )
                {
                    CommandResult result = client.execute(
                            new RequestBuilder( Commands.INSTALL_COMMAND ).withTimeout( 20000 ).withStdOutRedirection(
                                    OutputRedirection.NO ) );
                    checkInstalled( client, result );
                    trackerOperation.addLog( HBaseConfig.PRODUCT_KEY + " is installed on " + client.getHostname() );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Failed to install %s on server node", HBaseConfig.PRODUCT_KEY ) );
            }
        }

        try
        {
            new ClusterConfiguration( trackerOperation, manager, hadoop ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }


        trackerOperation.addLog( "Saving to db..." );
        try
        {
            manager.saveConfig( config );
            trackerOperation.addLog( "Installation info successfully saved" );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( "Failed to save installation info" );
        }
        return config;
    }


    private void checkConfig() throws ClusterSetupException
    {
        String m = "Invalid configuration: ";

        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            throw new ClusterSetupException( m + "Cluster name not specified" );
        }
    }


    public static Set<Host> getHosts( HBaseConfig config, Environment environment )
    {
        Set<Host> hosts = new HashSet<>();
        for ( String uuid : config.getAllNodes() )
        {
            try
            {
                hosts.add( environment.getContainerHostById( uuid ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        return hosts;
    }


    private boolean checkIfProductIsInstalled( EnvironmentContainerHost containerHost, String productName )
    {
        boolean isHbaseInstalled = false;
        try
        {
            CommandResult result = containerHost.execute( Commands.getCheckInstalledCommand() );
            if ( result.getStdOut().contains( productName ) )
            {
                isHbaseInstalled = true;
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return isHbaseInstalled;
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( Commands.getCheckInstalledCommand() );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && ( statusResult.getStdOut().contains( Commands.PACKAGE_NAME ) ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
