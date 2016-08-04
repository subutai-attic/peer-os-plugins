package io.subutai.plugin.cassandra.impl;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
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
        String script = "/etc/cassandra/cassandra-conf.sh %s";
        String permissionParam = "chmod +x /etc/cassandra/cassandra-conf.sh";
        String clusterNameParam = "cluster_name " + config.getClusterName();
        String dataDirParam = "data_dir " + config.getDataDirectory();
        String commitLogDirParam = "commitlog_dir " + config.getCommitLogDirectory();
        String savedCacheDirParam = "saved_cache_dir " + config.getSavedCachesDirectory();


        StringBuilder sb = new StringBuilder();
        for ( String id : config.getSeedNodes() )
        {
            EnvironmentContainerHost containerHost;
            try
            {
                containerHost = environment.getContainerHostById( id );
                sb.append( containerHost.getInterfaceByName( "eth0" ).getIp() ).append( "," );
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


        for ( String id : config.getNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );
                po.addLog( "Configuring node: " + containerHost.getHostname() );

                // Setting permission
                CommandResult commandResult = containerHost.execute( new RequestBuilder( permissionParam ) );
                po.addLog( commandResult.getStdOut() );

                // Setting cluster name
                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, clusterNameParam ) ) );
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
                commandResult = containerHost.execute( new RequestBuilder( String.format( script, dataDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, commitLogDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, savedCacheDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                // Set RPC address
                String rpcAddress = String.format( "/etc/cassandra/cassandra-conf.sh %s %s", "rpc_address",
                        containerHost.getInterfaceByName( "eth0" ).getIp() );
                commandResult = containerHost.execute( new RequestBuilder( rpcAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Set listen address
                String listenAddress = String.format( "/etc/cassandra/cassandra-conf.sh %s %s", "listen_address",
                        containerHost.getInterfaceByName( "eth0" ).getIp() );
                commandResult = containerHost.execute( new RequestBuilder( listenAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Configure seeds
                commandResult = containerHost.execute( new RequestBuilder( String.format( script, seedsParam ) ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Installation failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        for ( String id : config.getNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );
                po.addLog( "Configuring node: " + containerHost.getHostname() );

                // Stop cassandra service
                CommandResult commandResult = containerHost.execute( new RequestBuilder( Commands.stopCommand ) );
                po.addLog( commandResult.getStdOut() );

                // Remove /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.removeFolder ) );
                po.addLog( commandResult.getStdOut() );

                // Create /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.createFolder ) );
                po.addLog( commandResult.getStdOut() );

                // Chown /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.chown ) );
                po.addLog( commandResult.getStdOut() );

                // Restart cassandra service
                commandResult = containerHost.execute( new RequestBuilder( Commands.restartCommand ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Installation failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

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


    public void deleteClusterConfiguration( final CassandraClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        po.addLog( String.format( "Deleting configuration of cluster: %s", config.getClusterName() ) );
        String script = "/etc/cassandra/cassandra-conf.sh %s";
        String permissionParam = "chmod +x /etc/cassandra/cassandra-conf.sh";
        String clusterNameParam = "cluster_name Test Cluster";
        String dataDirParam = "data_dir /var/lib/cassandra/data";
        String commitLogDirParam = "commitlog_dir /var/lib/cassandra/commitlog";
        String savedCacheDirParam = "saved_cache_dir /var/lib/cassandra/saved_caches";

        String seedsParam = "seeds 127.0.0.1";

        for ( String id : config.getNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );

                // Setting permission
                CommandResult commandResult = containerHost.execute( new RequestBuilder( permissionParam ) );
                po.addLog( commandResult.getStdOut() );

                // Setting cluster name
                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, clusterNameParam ) ) );
                po.addLog( commandResult.getStdOut() );

                // Configure directories
                commandResult = containerHost.execute( new RequestBuilder( String.format( script, dataDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, commitLogDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                commandResult =
                        containerHost.execute( new RequestBuilder( String.format( script, savedCacheDirParam ) ) );
                po.addLog( commandResult.getStdOut() );

                // Set RPC address
                String rpcAddress =
                        String.format( "/etc/cassandra/cassandra-conf.sh %s %s", "rpc_address", "localhost" );
                commandResult = containerHost.execute( new RequestBuilder( rpcAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Set listen address
                String listenAddress =
                        String.format( "/etc/cassandra/cassandra-conf.sh %s %s", "listen_address", "localhost" );
                commandResult = containerHost.execute( new RequestBuilder( listenAddress ) );
                po.addLog( commandResult.getStdOut() );

                // Configure seeds
                commandResult = containerHost.execute( new RequestBuilder( String.format( script, seedsParam ) ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Deleting configuration failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        for ( String id : config.getNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );

                // Stop cassandra service
                CommandResult commandResult = containerHost.execute( new RequestBuilder( Commands.stopCommand ) );
                po.addLog( commandResult.getStdOut() );

                // Remove /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.removeFolder ) );
                po.addLog( commandResult.getStdOut() );

                // Create /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.createFolder ) );
                po.addLog( commandResult.getStdOut() );

                // Chown /var/lib/cassandra folder
                commandResult = containerHost.execute( new RequestBuilder( Commands.chown ) );
                po.addLog( commandResult.getStdOut() );

                // Restart cassandra service
                commandResult = containerHost.execute( new RequestBuilder( Commands.restartCommand ) );
                po.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                po.addLogFailed( "Installation failed" );
                throw new ClusterConfigurationException( e.getMessage() );
            }
        }

        try
        {
            cassandraManager.deleteConfig( config );
        }
        catch ( ClusterException e )
        {
            po.addLogFailed( "Failed to delete cluster information from database" );
            return;
        }

        po.addLogDone( "Cluster removed from database" );
    }
}
