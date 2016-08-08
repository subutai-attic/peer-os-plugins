package io.subutai.plugin.cassandra.impl;


import java.util.HashSet;
import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;


public class ClusterConfiguration
{

    private TrackerOperation po;
    private CassandraImpl cassandraManager;


    public ClusterConfiguration( final TrackerOperation operation, final CassandraImpl cassandraManager )
    {
        this.po = operation;
        this.cassandraManager = cassandraManager;
    }


    //TODO use host.getInterfaces instead of Agents
    public void configureCluster( CassandraClusterConfig config, Environment environment )
            throws ClusterConfigurationException
    {
        po.addLog( String.format( "Configuring cluster: %s", config.getClusterName() ) );
        String clusterNameParam = "cluster_name " + config.getClusterName();
        String dataDirParam = "data_dir " + config.getDataDirectory();
        String commitLogDirParam = "commitlog_dir " + config.getCommitLogDirectory();
        String savedCacheDirParam = "saved_cache_dir " + config.getSavedCachesDirectory();
        Set<ContainerHost> hosts = new HashSet<>();

        StringBuilder sb = new StringBuilder();
        for ( String id : config.getSeedNodes() )
        {
            EnvironmentContainerHost containerHost;
            try
            {
                containerHost = environment.getContainerHostByHostname( id );
                sb.append( containerHost.getInterfaceByName( "eth0" ).getIp() ).append( "," );
                hosts.add( containerHost );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterConfigurationException( e );
            }
        }
        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }
        String seedsParam = "seeds " + sb.toString();


        for ( String id : config.getSeedNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostByHostname( id );
                po.addLog( "Configuring node: " + containerHost.getHostname() );

                // Setting cluster name
                CommandResult commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, clusterNameParam ) ) );
                po.addLog( commandResult.getStdOut() );

                // Create directories
                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( "mkdir -p %s", config.getDataDirectory() ) ) );
                po.addLog( commandResult.getStdOut() );
                commandResult = containerHost.execute(
                        new RequestBuilder( String.format( "mkdir -p %s", config.getCommitLogDirectory() ) ) );
                po.addLog( commandResult.getStdOut() );
                commandResult = containerHost.execute(
                        new RequestBuilder( String.format( "mkdir -p %s", config.getSavedCachesDirectory() ) ) );
                po.addLog( commandResult.getStdOut() );

                // Configure directories
                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( Commands.SCRIPT, dataDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, commitLogDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, savedCacheDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                // Set RPC address
                String rpcAddress = String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "rpc_address",
                        containerHost.getInterfaceByName( "eth0" ).getIp() );
                commandResult = containerHost.execute( new RequestBuilder( rpcAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Set listen address
                String listenAddress = String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "listen_address",
                        containerHost.getInterfaceByName( "eth0" ).getIp() );
                commandResult = containerHost.execute( new RequestBuilder( listenAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Configure seeds
                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( Commands.SCRIPT, seedsParam ) ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Installation failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        restartNodes( hosts );

        config.setEnvironmentId( environment.getId() );

        try
        {
            cassandraManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterConfigurationException( e );
        }

        po.addLogDone( "Cassandra cluster data saved into database" );
    }


    private void restartNodes( final Set<ContainerHost> hosts ) throws ClusterConfigurationException
    {
        for ( ContainerHost host : hosts )
        {
            try
            {
                // Stop cassandra service
                CommandResult commandResult = host.execute( new RequestBuilder( Commands.STOP_COMMAND ) );
                po.addLog( commandResult.getStdOut() );

                // Remove /var/lib/cassandra folder
                commandResult = host.execute( new RequestBuilder( Commands.REMOVE_FOLDER ) );
                po.addLog( commandResult.getStdOut() );

                // Create /var/lib/cassandra folder
                commandResult = host.execute( new RequestBuilder( Commands.CREATE_FOLDER ) );
                po.addLog( commandResult.getStdOut() );

                // Chown /var/lib/cassandra folder
                commandResult = host.execute( new RequestBuilder( Commands.CHOWN ) );
                po.addLog( commandResult.getStdOut() );

                // Restart cassandra service
                commandResult = host.execute( new RequestBuilder( Commands.RESTART_COMMAND ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Installation failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }
    }


    public void deleteClusterConfiguration( final CassandraClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        po.addLog( String.format( "Deleting configuration of cluster: %s", config.getClusterName() ) );
        String seedsParam = "seeds 127.0.0.1";
        Set<ContainerHost> hosts = new HashSet<>();

        for ( String id : config.getSeedNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostByHostname( id );
                hosts.add( containerHost );

                // Setting cluster name
                CommandResult commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.CLUSTER_NAME_PARAM ) ) );
                po.addLog( commandResult.getStdOut() );

                // Configure directories
                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.DATA_DIR_DIR ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.COMMIT_LOG_DIR ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult = containerHost
                        .execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.SAVED_CHACHE_DIR ) ) );
                po.addLog( commandResult.getStdOut() );

                // Set RPC address
                String rpcAddress =
                        String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "rpc_address", "localhost" );
                commandResult = containerHost.execute( new RequestBuilder( rpcAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Set listen address
                String listenAddress =
                        String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "listen_address", "localhost" );
                commandResult = containerHost.execute( new RequestBuilder( listenAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Configure seeds
                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( Commands.SCRIPT, seedsParam ) ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Deleting configuration failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        restartNodes( hosts );

        try
        {
            cassandraManager.deleteConfig( config );
            po.addLogDone( "Cluster removed from database" );
        }
        catch ( ClusterException e )
        {
            po.addLogFailed( "Failed to delete cluster information from database" );
        }
    }


    public void removeNode( final EnvironmentContainerHost host ) throws ClusterConfigurationException
    {
        po.addLog( String.format( "Deleting configuration of cluster: %s", host.getHostname() ) );
        String seedsParam = "seeds 127.0.0.1";

        try
        {
            // Setting cluster name
            CommandResult commandResult =
                    host.execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.CLUSTER_NAME_PARAM ) ) );
            po.addLog( commandResult.getStdOut() );

            // Configure directories
            commandResult =
                    host.execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.DATA_DIR_DIR ) ) );
            po.addLog( commandResult.getStdOut() );

            commandResult =
                    host.execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.COMMIT_LOG_DIR ) ) );
            po.addLog( commandResult.getStdOut() );

            commandResult =
                    host.execute( new RequestBuilder( String.format( Commands.SCRIPT, Commands.SAVED_CHACHE_DIR ) ) );
            po.addLog( commandResult.getStdOut() );

            // Set RPC address
            String rpcAddress =
                    String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "rpc_address", "localhost" );
            commandResult = host.execute( new RequestBuilder( rpcAddress ) );
            po.addLog( commandResult.getStdOut() );

            // Set listen address
            String listenAddress =
                    String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "listen_address", "localhost" );
            commandResult = host.execute( new RequestBuilder( listenAddress ) );
            po.addLog( commandResult.getStdOut() );

            // Configure seeds
            commandResult = host.execute( new RequestBuilder( String.format( Commands.SCRIPT, seedsParam ) ) );
            po.addLog( commandResult.getStdOut() );

            // Stop cassandra service
            commandResult = host.execute( new RequestBuilder( Commands.STOP_COMMAND ) );
            po.addLog( commandResult.getStdOut() );

            // Remove /var/lib/cassandra folder
            commandResult = host.execute( new RequestBuilder( Commands.REMOVE_FOLDER ) );
            po.addLog( commandResult.getStdOut() );

            // Create /var/lib/cassandra folder
            commandResult = host.execute( new RequestBuilder( Commands.CREATE_FOLDER ) );
            po.addLog( commandResult.getStdOut() );

            // Chown /var/lib/cassandra folder
            commandResult = host.execute( new RequestBuilder( Commands.CHOWN ) );
            po.addLog( commandResult.getStdOut() );

            // Restart cassandra service
            commandResult = host.execute( new RequestBuilder( Commands.RESTART_COMMAND ) );
            po.addLog( commandResult.getStdOut() );
        }
        catch ( CommandException e )
        {
            po.addLogFailed( "Installation failed" );
            throw new ClusterConfigurationException( e.getMessage() );
        }
    }


    public void addNode( final CassandraClusterConfig config, final Environment environment,
                         final EnvironmentContainerHost newNode ) throws ClusterConfigurationException
    {
        po.addLog( String.format( "Configuring cluster: %s", config.getClusterName() ) );
        String clusterNameParam = "cluster_name " + config.getClusterName();
        String dataDirParam = "data_dir " + config.getDataDirectory();
        String commitLogDirParam = "commitlog_dir " + config.getCommitLogDirectory();
        String savedCacheDirParam = "saved_cache_dir " + config.getSavedCachesDirectory();
        Set<ContainerHost> hosts = new HashSet<>();

        StringBuilder sb = new StringBuilder();
        for ( String id : config.getSeedNodes() )
        {
            EnvironmentContainerHost containerHost;
            try
            {
                containerHost = environment.getContainerHostByHostname( id );
                sb.append( containerHost.getInterfaceByName( "eth0" ).getIp() ).append( "," );
                hosts.add( containerHost );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterConfigurationException( e );
            }
        }
        if ( !sb.toString().isEmpty() )
        {
            sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
        }
        String seedsParam = "seeds " + sb.toString();


        try
        {
            po.addLog( "Configuring new node: " + newNode.getHostname() );

            // Setting cluster name
            CommandResult commandResult =
                    newNode.execute( new RequestBuilder( String.format( Commands.SCRIPT, clusterNameParam ) ) );
            po.addLog( commandResult.getStdOut() );

            // Create directories
            commandResult =
                    newNode.execute( new RequestBuilder( String.format( "mkdir -p %s", config.getDataDirectory() ) ) );
            po.addLog( commandResult.getStdOut() );
            commandResult = newNode.execute(
                    new RequestBuilder( String.format( "mkdir -p %s", config.getCommitLogDirectory() ) ) );
            po.addLog( commandResult.getStdOut() );
            commandResult = newNode.execute(
                    new RequestBuilder( String.format( "mkdir -p %s", config.getSavedCachesDirectory() ) ) );
            po.addLog( commandResult.getStdOut() );

            // Configure directories
            commandResult = newNode.execute( new RequestBuilder( String.format( Commands.SCRIPT, dataDirParam ) ) );
            po.addLog( commandResult.getStdOut() );

            commandResult =
                    newNode.execute( new RequestBuilder( String.format( Commands.SCRIPT, commitLogDirParam ) ) );
            po.addLog( commandResult.getStdOut() );

            commandResult =
                    newNode.execute( new RequestBuilder( String.format( Commands.SCRIPT, savedCacheDirParam ) ) );
            po.addLog( commandResult.getStdOut() );

            // Set RPC address
            String rpcAddress = String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "rpc_address",
                    newNode.getInterfaceByName( "eth0" ).getIp() );
            commandResult = newNode.execute( new RequestBuilder( rpcAddress ) );
            po.addLog( commandResult.getStdOut() );

            // Set listen address
            String listenAddress = String.format( "bash /etc/cassandra/cassandra-conf.sh %s %s", "listen_address",
                    newNode.getInterfaceByName( "eth0" ).getIp() );
            commandResult = newNode.execute( new RequestBuilder( listenAddress ) );
            po.addLog( commandResult.getStdOut() );

            for ( final ContainerHost host : hosts )
            {
                // Configure seeds
                commandResult = host.execute( new RequestBuilder( String.format( Commands.SCRIPT, seedsParam ) ) );
                po.addLog( commandResult.getStdOut() );
            }
        }
        catch ( CommandException e )
        {
            po.addLogFailed( "Installation failed" );
            throw new ClusterConfigurationException( e.getMessage() );
        }

        restartNodes( hosts );
    }
}



