package io.subutai.plugin.mongodb.impl;


import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
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


    public void configureCluster( MongoClusterConfig config, Environment environment )
            throws ClusterConfigurationException
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
                configureCofigServer( configServer );
            }

            int count = 0;
            StringBuilder sb = new StringBuilder();
            for ( final EnvironmentContainerHost configServer : configServers )
            {
                sb.append( String.format( "{_id: %s, host:\\\"%s:27019\\\"}", String.valueOf( count ),
                        configServer.getHostname() ) ).append( "," );
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
            mongosRouter.execute(
                    Commands.getSetShardCommand( primaryDataNode.getHostname(), config.getReplicaSetName() ) );
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
        mongosNode.execute( Commands.getChangeServiceCommand( "mongod", "mongos" ) );

        // Restart mongos
        mongosNode.execute( Commands.getMongosRestartCommand() );
    }


    private void configureCofigServer( final EnvironmentContainerHost configServer ) throws CommandException
    {
        // Changing bindip to 0.0.0.0
        configServer.execute( Commands.getSetBindIpCommand() );

        // Changing port to 27019
        configServer.execute( Commands.getSetPortCommand() );

        // Setting Replication
        configServer.execute( Commands.getReplSetCommand( "configReplSet" ) );

        // Setting cluster role
        configServer.execute( Commands.getSetClusterRoleCommand() );

        // Restart node
        configServer.execute( Commands.getMongoDBRestartCommand() );
    }


    public void configureDataNodes( EnvironmentContainerHost host, final String replicaSetName ) throws MongoException
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


    public EnvironmentContainerHost findContainerHost( String id, Environment environment )
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
                host.execute( Commands.getShutDownCommand() );
                // Reset config file
                host.execute( Commands.getResetMongosConfig() );
                // Stop mongodb
                host.execute( Commands.getMongosStopCommand() );
                // Rename service to mongos
                host.execute( Commands.getRenametoMongodbCommand() );
                // Change to mongos in service file
                host.execute( Commands.getChangeServiceCommand( "mongos", "mongod" ) );
                // Reload daemon
                host.execute( Commands.getReloadDaemonCommand() );
                // Restart node
                host.execute( Commands.getMongosRestartCommand() );
                break;
            case DATA_NODE:
                // Shutdown node
                host.execute( Commands.getShutDownCommand() );
                EnvironmentContainerHost primaryDataNode =
                        findContainerHost( config.getPrimaryDataNode(), environment );
                // remove from replicaSet
                primaryDataNode.execute( Commands.getRemoveFromReplicaSetCommand( primaryDataNode.getHostname() ) );
                // reset config file
                host.execute( Commands.getResetDataConfig() );
                // restart node
                host.execute( Commands.getMongoDBRestartCommand() );
                break;
        }
    }


    public void addNode( final MongoClusterConfig config, final Environment environment,
                         final EnvironmentContainerHost newNode, final NodeType nodeType )
            throws CommandException, MongoException
    {
        // TODO check adding new nodes
        switch ( nodeType )
        {
            case ROUTER_NODE:
                configureMongosNodes( newNode, config.getPrimaryConfigServer() );
                newNode.execute( Commands.getSetShardCommand(
                        findContainerHost( config.getPrimaryDataNode(), environment ).getHostname(),
                        config.getReplicaSetName() ) );

                break;
            case DATA_NODE:
                configureDataNodes( newNode, config.getReplicaSetName() );
                findContainerHost( config.getPrimaryDataNode(), environment )
                        .execute( Commands.getAddDataReplCommand( newNode.getHostname() ) );
                break;
        }
    }
}
