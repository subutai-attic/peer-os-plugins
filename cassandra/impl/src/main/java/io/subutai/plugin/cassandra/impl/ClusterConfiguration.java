package io.subutai.plugin.cassandra.impl;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.digester.plugins.PluginConfigurationException;
import org.apache.commons.lang3.StringUtils;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;


public class ClusterConfiguration
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );
    private static final long MB = 1024 * 1024;
    private static final long GB = MB * 1024;
    private static final String CHAIN_AND_OPERATOR = " && ";
    public static final String NET_INTERFACE = "eth0";
    private static final int NODE_WAIT_TIMEOUT = 65;

    private TrackerOperation po;
    private CassandraImpl cassandraManager;


    public ClusterConfiguration( final TrackerOperation operation, final CassandraImpl cassandraManager )
    {
        this.po = operation;
        this.cassandraManager = cassandraManager;
    }


    public void configureCluster( CassandraClusterConfig config, Environment environment )
            throws ClusterConfigurationException
    {
        po.addLog( String.format( "Configuring cluster: %s", config.getClusterName() ) );

        Set<EnvironmentContainerHost> allNodes = environment.getContainerHosts();

        if ( allNodes.size() <= 1 )
        {
            return;
        }

        // copy seed nodes to list
        List<String> seedNodes = new ArrayList<>();
        seedNodes.addAll( config.getSeedNodes() );

        // clear seed nodes from configuration
        config.setSeedNodes( new HashSet<String>() );

        // up seed nodes
        for ( String nodeName : seedNodes )
        {
            try
            {
                EnvironmentContainerHost node = environment.getContainerHostByHostname( nodeName );
                config.addSeedNode( nodeName );
                addNode( config, environment, node );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( e.getMessage(), e );
                po.addLogFailed( e.getMessage() );
                throw new ClusterConfigurationException( e );
            }
        }
        Iterator<EnvironmentContainerHost> iterator = allNodes.iterator();

        config.setEnvironmentId( environment.getId() );

        try
        {

            while ( iterator.hasNext() )
            {
                EnvironmentContainerHost node = iterator.next();

                if ( !seedNodes.contains( node.getHostname() ) )
                {
                    config.addSeedNode( node.getHostname() );
                    addNode( config, environment, node );
                }
            }
            cassandraManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            LOG.error( e.getMessage(), e );
            po.addLogFailed( e.getMessage() );
            throw new ClusterConfigurationException( e );
        }


        po.addLogDone( "Cassandra cluster data saved into database" );
    }


    public void addNode( final CassandraClusterConfig config, final Environment environment,
                         final EnvironmentContainerHost node ) throws ClusterConfigurationException
    {
        LOG.debug( "Trying to configure Cassandra on {}.", node.getId() );

        final String[] commands = getCommands( node, config );
        po.addLog( String.format( "Trying to configure Cassandra on %s}.", node.getId() ) );
        try
        {
            CommandResult response = execute( node, NODE_WAIT_TIMEOUT, Commands.getAvailableRam() );
            String ramLimitStr = response.getStdOut().trim();
            LOG.debug( "RAM limit on {}: {}.", node.getId(), ramLimitStr );

            Long ramLimit = Long.valueOf( ramLimitStr );

            //            max(min(1/2 ram, 1024MB), min(1/4 ram, 8GB))
            if ( ramLimit < 2 * GB )
            {
                throw new Exception(
                        String.format( "Insufficient memory on %s. Current available memory: %d", node.getId(),
                                ramLimit ) );
            }

            long heapSize = Double.valueOf(
                    Math.max( Math.min( ramLimit * 0.5, 1024 * MB ), Math.min( ramLimit * 0.25, 8 * GB ) ) )
                                  .longValue();

            if ( heapSize < GB )
            {
                throw new Exception(
                        String.format( "Insufficient memory on %s. Current available memory: %d", node.getId(),
                                ramLimit ) );
            }
            long heapSizeInMb = heapSize / MB;
            response = execute( node, NODE_WAIT_TIMEOUT, Commands.getHeapSize( heapSizeInMb ) );
            if ( response.getExitCode() == 0 )
            {
                LOG.debug( "Now heap size of {}={}Mb.", node.getId(), heapSizeInMb );
            }
            else
            {
                LOG.debug( "Could not set heap size of node {} to {}Mb.", node.getId(), heapSize );
            }

            // Create directories
            CommandResult commandResult =
                    node.execute( new RequestBuilder( String.format( "mkdir -p %s", config.getDataDirectory() ) ) );
            po.addLog( commandResult.getStdOut() );
            commandResult = node.execute(
                    new RequestBuilder( String.format( "mkdir -p %s", config.getCommitLogDirectory() ) ) );
            po.addLog( commandResult.getStdOut() );
            commandResult = node.execute(
                    new RequestBuilder( String.format( "mkdir -p %s", config.getSavedCachesDirectory() ) ) );

            po.addLog( commandResult.getStdOut() );

            execute( node, NODE_WAIT_TIMEOUT, commands );

            long waitThreshold = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( NODE_WAIT_TIMEOUT );

            String regex = String.format( "^UN\\s*%s.*", node.getInterfaceByName( NET_INTERFACE ).getIp() );

            while ( waitThreshold > System.currentTimeMillis() )
            {
                try
                {
                    TimeUnit.SECONDS.sleep( 15 );
                }
                catch ( InterruptedException ignore )
                {
                    //ignore
                }

                response = null;
                try
                {
                    response = execute( node, NODE_WAIT_TIMEOUT, Commands.getNodetoolStatus() );
                }
                catch ( Exception ignore )
                {
                    //ignore
                }

                if ( response != null && response.getExitCode() == 0 && response.getStdOut() != null )
                {
                    List<String> status = getClusterStatus( response.getStdOut() );

                    for ( String s : status )
                    {
                        if ( s.matches( regex ) )
                        {
                            LOG.debug( "C* successfully started on {}.", node.getId() );
                            po.addLog( String.format( "Configure Cassandra process succeeded on %s.", node.getId() ) );
                            return;
                        }
                    }
                }
            }

            LOG.debug( "Cassandra configuration on {} failed.", node.getId() );
            final CommandResult errResponse = execute( node, 15, Commands.getHsErrors() );

            LOG.debug( "HR_ERR: {}.", errResponse.getStdOut() );
        }
        catch ( Exception e )
        {
            LOG.debug( "HR_ERR: {}", e.getMessage() );
        }

        throw new PluginConfigurationException( "Cassandra configuration failed on : " + node.getId() );
    }


    private CommandResult execute( final EnvironmentContainerHost node, final int timeout, final String command )
            throws CommandException
    {
        RequestBuilder requestBuilder = new RequestBuilder( command ).withTimeout( timeout );
        return node.execute( requestBuilder );
    }


    private CommandResult execute( final EnvironmentContainerHost node, final int timeout, final String[] commands )
            throws CommandException
    {
        RequestBuilder requestBuilder =
                new RequestBuilder( StringUtils.join( commands, CHAIN_AND_OPERATOR ) ).withTimeout( timeout );
        return node.execute( requestBuilder );
    }


    private List<String> getClusterStatus( String output )
    {
        final List<String> result = new ArrayList<>();


        BufferedReader input = new BufferedReader( new StringReader( output ) );
        String line = null;

        try
        {
            while ( ( line = input.readLine() ) != null )
            {
                result.add( line );
            }
        }
        catch ( IOException e )
        {
            LOG.error( e.getMessage() );
        }

        return result;
    }


    private String[] getCommands( final EnvironmentContainerHost node, final CassandraClusterConfig config )
    {
        String ipAddress = node.getInterfaceByName( "eth0" ).getIp();
        String seedNodes = StringUtils.join( config.getSeedNodes(), "," );
        String clusterNameParam = "cluster_name " + config.getClusterName();
        String dataDirParam = "data_dir " + config.getDataDirectory();
        String commitLogDirParam = "commitlog_dir " + config.getCommitLogDirectory();
        String savedCacheDirParam = "saved_cache_dir " + config.getSavedCachesDirectory();

        return new String[] {
                Commands.getReplacePropertyCommand( clusterNameParam ), Commands.getSeedsCommand( seedNodes ),
                Commands.getListenAddressCommand( ipAddress ), Commands.getRpcAddressCommand( ipAddress ),
                Commands.getEndpointSnitchCommand(), Commands.getReplacePropertyCommand( dataDirParam ),
                Commands.getReplacePropertyCommand( commitLogDirParam ),
                Commands.getReplacePropertyCommand( savedCacheDirParam ), Commands.getAutoBootstrapCommand(),
                Commands.getStopCommand(), Commands.getRemoveFolderCommand(), Commands.getCreateFolderCommand(),
                Commands.getChownFolderCommand(), Commands.getRestartCommand()
        };
    }


    //TODO use host.getInterfaces instead of Agents
    public void configureClusterOld( CassandraClusterConfig config, Environment environment )
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
        po.addLog( String.format( "Deleting configuration of node: %s", host.getHostname() ) );
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


    public void addNodeOld( final CassandraClusterConfig config, final Environment environment,
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



