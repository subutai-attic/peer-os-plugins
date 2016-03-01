package io.subutai.plugin.hive.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hive.api.HiveConfig;


public class HiveSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger( HiveSetupStrategy.class );
    public final HiveImpl hiveManager;
    public final HiveConfig config;
    public final TrackerOperation trackerOperation;
    private Environment environment;
    private EnvironmentContainerHost server;
    private Set<EnvironmentContainerHost> clients;


    public HiveSetupStrategy( HiveImpl manager, HiveConfig config, TrackerOperation trackerOperation )
    {
        this.hiveManager = manager;
        this.config = config;
        this.trackerOperation = trackerOperation;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();

        return config;
    }


    private void check() throws ClusterSetupException
    {

        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            throw new ClusterSetupException( "Cluster name not specified" );
        }
        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            throw new ClusterSetupException( "Hadoop cluster name not specified" );
        }

        if ( hiveManager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        final HadoopClusterConfig hadoopClusterConfig =
                hiveManager.getHadoopManager().getCluster( config.getHadoopClusterName() );

        if ( hadoopClusterConfig == null )
        {
            throw new ClusterSetupException(
                    String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( config.getAllNodes() ) )
        {
            throw new ClusterSetupException( String.format( "Not all nodes belong to Hadoop cluster %s",
                    hadoopClusterConfig.getClusterName() ) );
        }

        try
        {
            environment = hiveManager.getEnvironmentManager().loadEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment by id: " + hadoopClusterConfig.getEnvironmentId(), e );
            return;
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        if ( config.getServer() == null )
        {
            throw new ClusterSetupException( "Server node not specified" );
        }

        if ( CollectionUtil.isCollectionEmpty( config.getClients() ) )
        {
            throw new ClusterSetupException( "Target nodes not specified" );
        }


        Set<EnvironmentContainerHost> hiveNodes = null;
        try
        {
            hiveNodes = environment.getContainerHostsByIds( config.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }
        if ( hiveNodes != null )
        {
            if ( hiveNodes.size() < config.getAllNodes().size() )
            {
                throw new ClusterSetupException(
                        String.format( "Only %d nodes found in environment whereas %d expected", hiveNodes.size(),
                                config.getAllNodes().size() ) );
            }
        }
        else
        {
            throw new ClusterSetupException( String.format( "No nodes found in environment" ) );
        }


        for ( EnvironmentContainerHost hiveNode : hiveNodes )
        {
            if ( !hiveNode.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", hiveNode.getHostname() ) );
            }
        }

        try
        {
            server = environment.getContainerHostById( config.getServer() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Container host not found" + config.getServer(), e );
            trackerOperation.addLogFailed( "Container host not found" );
        }

        try
        {
            clients = environment.getContainerHostsByIds( config.getClients() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Container hosts not found" + config.getClients(), e );
            trackerOperation.addLogFailed( "Containers host not found" );
        }
    }


    private void configure() throws ClusterSetupException
    {
        // installation of server
        trackerOperation.addLog( "Installing server..." );
        try
        {
            if ( !checkIfProductIsInstalled( server, HiveConfig.PRODUCT_KEY.toLowerCase() ) )
            {
                server.execute( new RequestBuilder(
                        Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() )
                        .withTimeout( 600 ) );
            }
            if ( !checkIfProductIsInstalled( server, "derby" ) )
            {
                server.execute( new RequestBuilder( Commands.installCommand + Common.PACKAGE_PREFIX + "derby" )
                        .withTimeout( 600 ) );
            }
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Failed to install derby on server node !!! " ) );
        }

        trackerOperation.addLog( "Server installation completed" );


        // installation of clients
        trackerOperation.addLog( "Installing clients..." );
        for ( EnvironmentContainerHost client : clients )
        {
            try
            {
                if ( !checkIfProductIsInstalled( client, HiveConfig.PRODUCT_KEY.toLowerCase() ) )
                {
                    CommandResult result = client.execute( new RequestBuilder(
                            Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() )
                            .withTimeout( 600 ) );
                    checkInstalled( client, result );
                    trackerOperation.addLog( HiveConfig.PRODUCT_KEY + " is installed on " + client.getHostname() );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Failed to install %s on server node", HiveConfig.PRODUCT_KEY ) );
            }
        }

        try
        {
            new ClusterConfiguration( hiveManager, trackerOperation ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }
    }


    private boolean checkIfProductIsInstalled( EnvironmentContainerHost containerHost, String productName )
    {
        boolean isHiveInstalled = false;
        try
        {
            CommandResult result = containerHost.execute( new RequestBuilder( Commands.checkIfInstalled ) );
            if ( result.getStdOut().contains( productName ) )
            {
                isHiveInstalled = true;
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return isHiveInstalled;
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( new RequestBuilder( Commands.checkIfInstalled ) );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && ( statusResult.getStdOut()
                                                       .contains( HiveConfig.PRODUCT_KEY.toLowerCase() ) ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
