package io.subutai.plugin.mongodb.impl;


import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.MongoException;
import io.subutai.plugin.mongodb.api.NodeType;
import io.subutai.plugin.mongodb.impl.common.Commands;


public class ClusterConfiguration
{
    private TrackerOperation po;
    private MongoImpl mongoManager;
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );


    public ClusterConfiguration( final TrackerOperation operation, final MongoImpl mongoManager )
    {
        this.po = operation;
        this.mongoManager = mongoManager;
    }


    void configureCluster( MongoClusterConfig config, Environment environment ) throws ClusterConfigurationException
    {
        po.addLog( String.format( "Configuring cluster: %s", config.getClusterName() ) );
        try
        {
            // Setting locale to all nodes
            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );

            for ( final EnvironmentContainerHost node : allNodes )
            {
                node.execute( Commands.getSetLocaleCommand() );
            }

            // Configuring Config servers
            Set<EnvironmentContainerHost> configServers = environment.getContainerHostsByIds( config.getConfigHosts() );

            for ( final EnvironmentContainerHost configServer : configServers )
            {
                configureCofigServer( configServer, config.getCfgSrvPort() );
            }

            int count = 0;
            StringBuilder sb = new StringBuilder();
            for ( final EnvironmentContainerHost configServer : configServers )
            {
                sb.append( String.format( "{_id: %s, host:\\\"%s:%s\\\"}", String.valueOf( count ),
                        configServer.getHostname(), config.getCfgSrvPort() ) ).append( "," );
                count++;
            }

            if ( !sb.toString().isEmpty() )
            {
                sb.replace( sb.toString().length() - 1, sb.toString().length(), "" );
            }

            String servers = sb.toString();

            EnvironmentContainerHost primaryConfigServer = configServers.iterator().next();
            primaryConfigServer.execute( Commands.getAddConfigReplCommand( servers ) );

            config.setPrimaryConfigServer( primaryConfigServer.getId() );

            // Configuring Data nodes
            // Configuring /etc/mongod.conf of data nodes
            Set<EnvironmentContainerHost> dataNodes = environment.getContainerHostsByIds( config.getDataHosts() );

            for ( final EnvironmentContainerHost dataNode : dataNodes )
            {
                po.addLog( "Setting replicaSetname: " + dataNode.getHostname() );
                configureDataNodes( dataNode, config.getReplicaSetName() );
            }

            // Configuring primary data node
            EnvironmentContainerHost primaryDataNode = dataNodes.iterator().next();

            primaryDataNode.execute( new RequestBuilder( "sleep 5" ) );
            primaryDataNode.execute( Commands.getReplInitiateCommand() );

            config.setPrimaryDataNode( primaryDataNode.getId() );

            // Adding secondary data nodes
            for ( final EnvironmentContainerHost dataNode : dataNodes )
            {
                if ( !Objects.equals( dataNode.getId(), primaryDataNode.getId() ) )
                {
                    primaryDataNode.execute( Commands.getAddDataReplCommand( dataNode.getHostname() ) );
                }
            }

            // Configuring mongos (routers)
            Set<EnvironmentContainerHost> mongosNodes = environment.getContainerHostsByIds( config.getRouterHosts() );

            for ( final EnvironmentContainerHost mongosNode : mongosNodes )
            {
                configureMongosNodes( mongosNode, primaryConfigServer.getHostname() );
            }

            EnvironmentContainerHost mongosRouter = mongosNodes.iterator().next();

            mongosRouter.execute( new RequestBuilder( "sleep 5" ) );
            mongosRouter.execute(
                    Commands.getSetShardCommand( primaryDataNode.getHostname(), config.getReplicaSetName(),
                            config.getDataNodePort() ) );
        }
        catch ( MongoException e )
        {
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }

        config.setEnvironmentId( environment.getId() );


        try
        {
            mongoManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterConfigurationException( e );
        }
        po.addLogDone( "MongoDB cluster data saved into database" );
    }


    private void configureMongosNodes( final EnvironmentContainerHost mongosNode, final String primaryConfigServer )
            throws CommandException
    {
        // Changing bindip to 0.0.0.0
        mongosNode.execute( Commands.getSetBindIpCommand() );

        // Comment storage section
        mongosNode.execute( Commands.getCommentStorageCommand() );

        // Set configDB
        mongosNode.execute( Commands.getSetConfigDbCommand( primaryConfigServer ) );

        // Stop mongodb
        mongosNode.execute( Commands.getMongodbStopCommand() );

        // Rename service to mongos
        mongosNode.execute( Commands.getRenametoMongosCommand() );

        // Change to mongos in service file
        mongosNode.execute( Commands.getChangeServiceToMongosCommand( "mongos" ) );

        // Reload daemon
        mongosNode.execute( Commands.getReloadDaemonCommand() );

        // Restart mongos
        mongosNode.execute( Commands.getMongosRestartCommand() );
    }


    private void configureCofigServer( final EnvironmentContainerHost configServer, final int cfgSrvPort )
            throws CommandException
    {
        // Changing bindip to 0.0.0.0
        configServer.execute( Commands.getSetBindIpCommand() );

        // Changing port to 27019
        configServer.execute( Commands.getSetPortCommand( String.valueOf( cfgSrvPort ) ) );

        // Setting Replication
        configServer.execute( Commands.getReplSetCommand( "configReplSet" ) );

        // Setting cluster role
        configServer.execute( Commands.getSetClusterRoleCommand() );

        // Restart node
        configServer.execute( Commands.getMongoDBRestartCommand() );
    }


    private void configureDataNodes( EnvironmentContainerHost host, final String replicaSetName ) throws MongoException
    {
        try
        {
            // Changing bindip to 0.0.0.0
            host.execute( Commands.getSetBindIpCommand() );

            // Setting replSetName
            host.execute( Commands.getReplSetCommand( replicaSetName ) );

            // Restart node
            host.execute( Commands.getMongoDBRestartCommand() );
        }
        catch ( CommandException e )
        {
            throw new MongoException( "Error on setting replica set name: " );
        }
    }


    private EnvironmentContainerHost findContainerHost( String id, Environment environment )
    {
        try
        {
            return environment.getContainerHostById( id );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }


    public void removeNode( final MongoClusterConfig config, final Environment environment,
                            final EnvironmentContainerHost host, final NodeType nodeType ) throws CommandException
    {
        switch ( nodeType )
        {
            case ROUTER_NODE:
                // Shutdown node
                host.execute( Commands.getShutDownCommand( config.getCfgSrvPort() ) );
                // Reset config file
                host.execute( Commands.getResetMongosConfig() );
                // Reset port
                host.execute( Commands.getSetPortCommand( "27017" ) );
                // Stop mongodb
                host.execute( Commands.getMongosStopCommand() );
                // Rename service to mongos
                host.execute( Commands.getRenametoMongodbCommand() );
                // Change to mongos in service file
                host.execute( Commands.getChangeServiceToMongodCommand( "mongod" ) );
                // Reload daemon
                host.execute( Commands.getReloadDaemonCommand() );
                // Update var dir
                host.execute( Commands.getUpdateVarDirectory() );
                // Restart node
                host.execute( Commands.getMongoDBRestartCommand() );
                break;
            case DATA_NODE:
                // Shutdown node
                host.execute( Commands.getShutDownCommand( config.getDataNodePort() ) );
                EnvironmentContainerHost primaryDataNode =
                        findContainerHost( config.getPrimaryDataNode(), environment );
                // remove from replicaSet
                if ( primaryDataNode != null )
                {
                    primaryDataNode.execute(
                            Commands.getRemoveFromReplicaSetCommand( host.getHostname(), config.getDataNodePort() ) );
                }
                // reset config file
                host.execute( Commands.getResetDataConfig() );
                // Update var dir
                host.execute( Commands.getUpdateVarDirectory() );
                // restart node
                host.execute( Commands.getMongoDBRestartCommand() );
                break;
        }
    }


    public void addNode( final MongoClusterConfig config, final Environment environment,
                         final EnvironmentContainerHost newNode, final NodeType nodeType )
            throws CommandException, MongoException
    {
        switch ( nodeType )
        {
            case ROUTER_NODE:
                configureMongosNodes( newNode,
                        findContainerHost( config.getPrimaryConfigServer(), environment ).getHostname() );
                //                newNode.execute( Commands.getSetShardCommand(
                //                        findContainerHost( config.getPrimaryDataNode(), environment ).getHostname(),
                //                        config.getReplicaSetName() ) );

                break;
            case DATA_NODE:
                configureDataNodes( newNode, config.getReplicaSetName() );
                findContainerHost( config.getPrimaryDataNode(), environment )
                        .execute( Commands.getAddDataReplCommand( newNode.getHostname() ) );
                break;
        }
    }


    public void destroyCluster( final Set<EnvironmentContainerHost> dataNodes,
                                final Set<EnvironmentContainerHost> configServers,
                                final Set<EnvironmentContainerHost> routerNodes, final MongoClusterConfig config,
                                final Environment environment ) throws CommandException
    {
        for ( final EnvironmentContainerHost dataNode : dataNodes )
        {
            dataNode.execute( Commands.getResetDataConfig() );
            dataNode.execute( Commands.getUpdateVarDirectory() );
            dataNode.execute( Commands.getMongoDBRestartCommand() );
        }

        for ( final EnvironmentContainerHost configServer : configServers )
        {
            configServer.execute( Commands.getResetDataConfig() );
            configServer.execute( new RequestBuilder(
                    "sed -i -e 's/.*sharding:/#sharding:/g; s/.*clusterRole:.*/ /g' /etc/mongod.conf" ) );
            configServer.execute( Commands.getSetPortCommand( "27017" ) );
            configServer.execute( Commands.getUpdateVarDirectory() );
            configServer.execute( Commands.getMongoDBRestartCommand() );
        }
        for ( final EnvironmentContainerHost routerNode : routerNodes )
        {
            removeNode( config, environment, routerNode, NodeType.ROUTER_NODE );
        }
    }
}
