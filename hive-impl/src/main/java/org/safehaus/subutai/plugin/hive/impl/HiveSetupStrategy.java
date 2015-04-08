package org.safehaus.subutai.plugin.hive.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


public class HiveSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger( HiveSetupStrategy.class );
    public final HiveImpl hiveManager;
    public final HiveConfig config;
    public final TrackerOperation trackerOperation;
    private Environment environment;
    private ContainerHost server;
    private Set<ContainerHost> clients;


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
            environment = hiveManager.getEnvironmentManager().findEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment by id: " + hadoopClusterConfig.getEnvironmentId().toString(), e );
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


        Set<ContainerHost> hiveNodes = null;
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


        for ( ContainerHost hiveNode : hiveNodes )
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
            LOGGER.error( "Container host not found" + config.getServer().toString(), e );
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
                        Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ).withTimeout( 600 ) );
            }
            if ( !checkIfProductIsInstalled( server, "derby" ) )
            {
                server.execute( new RequestBuilder( Commands.installCommand + Common.PACKAGE_PREFIX + "derby" ).withTimeout( 600 ) );
            }
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Failed to install derby on server node !!! " ) );
        }

        trackerOperation.addLog( "Server installation completed" );


        // installation of clients
        trackerOperation.addLog( "Installing clients..." );
        for ( ContainerHost client : clients )
        {
            try
            {
                if ( !checkIfProductIsInstalled( client, HiveConfig.PRODUCT_KEY.toLowerCase() ) )
                {
                    CommandResult result = client.execute( new RequestBuilder(
                            Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ).withTimeout( 600 ) );
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


    private boolean checkIfProductIsInstalled( ContainerHost containerHost, String productName )
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


    public void checkInstalled( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( new RequestBuilder( Commands.checkIfInstalled ) );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname()) );
        }

        if ( !( result.hasSucceeded() && (statusResult.getStdOut().contains( HiveConfig.PRODUCT_KEY.toLowerCase()) ) )  )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
