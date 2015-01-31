package org.safehaus.subutai.plugin.hive.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
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
import org.safehaus.subutai.common.environment.Environment;

import com.google.common.base.Strings;


public class HiveSetupStrategy implements ClusterSetupStrategy
{
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
            environment =
                    hiveManager.getEnvironmentManager().findEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
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
            e.printStackTrace();
        }
        if ( hiveNodes.size() < config.getAllNodes().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Only %d nodes found in environment whereas %d expected", hiveNodes.size(),
                            config.getAllNodes().size() ) );
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
            e.printStackTrace();
        }

        try
        {
            clients = environment.getContainerHostsByIds( config.getClients() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
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
                        Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ) );
            }
            if ( !checkIfProductIsInstalled( server, "derby" ) )
            {
                server.execute( new RequestBuilder( Commands.installCommand + Common.PACKAGE_PREFIX + "derby" ) );
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
                    client.execute( new RequestBuilder(
                            Commands.installCommand + Common.PACKAGE_PREFIX + HiveConfig.PRODUCT_KEY.toLowerCase() ) );
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
}
