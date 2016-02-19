package io.subutai.plugin.mongodb.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.MongoException;
import io.subutai.plugin.mongodb.impl.common.CommandDef;
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
            for ( String id : config.getDataHosts() )
            {
                EnvironmentContainerHost dataNode = findContainerHost( id, environment );
                po.addLog( "Setting replicaSetname: " + dataNode.getHostname() );
                setReplicaSetName( dataNode, config.getReplicaSetName() );
            }

            EnvironmentContainerHost datanode;
            for ( String id : config.getDataHosts() )
            {
                datanode = findContainerHost( id, environment );
                if ( config.getPrimaryNode() == null )
                {
                    config.setPrimaryNode( datanode.getId() );
                    initiateReplicaSet( datanode, config );
                    po.addLog( "Primary data node: " + datanode.getHostname() );
                }
                else
                {
                    po.addLog( "registering secondary data node: " + datanode.getHostname() );
                    registerSecondaryNode( datanode, config );
                }
            }
        }
        catch ( MongoException e )
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


    public void setReplicaSetName( EnvironmentContainerHost host, final String replicaSetName ) throws MongoException
    {
        try
        {
            CommandDef commandDef = Commands.getSetReplicaSetNameCommandLine( replicaSetName );
            host.execute( commandDef.build().withTimeout( 90 ) );
        }
        catch ( CommandException e )
        {
            throw new MongoException( "Error on setting replica set name: " );
        }
    }


    public void initiateReplicaSet( EnvironmentContainerHost host, MongoClusterConfig config ) throws MongoException
    {
        CommandDef commandDef = Commands.getInitiateReplicaSetCommandLine( config.getCfgSrvPort() );
        try
        {
            CommandResult commandResult = host.execute( commandDef.build().withTimeout( 90 ) );

            if ( !commandResult.getStdOut().contains( "connecting to:" ) )
            {
                throw new CommandException( "Could not register secondary node." );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( commandDef.getDescription(), e );
            throw new MongoException( "Initiate replica set error." );
        }
    }


    public void registerSecondaryNode( final EnvironmentContainerHost dataNode, MongoClusterConfig config ) throws MongoException
    {
        CommandDef commandDef = Commands.getRegisterSecondaryNodeWithPrimaryCommandLine( dataNode.getHostname(),
                config.getDataNodePort(), config.getDomainName() );
        try
        {
            CommandResult commandResult = dataNode.execute( commandDef.build().withTimeout( 90 ) );

            if ( !commandResult.getStdOut().contains( "connecting to:" ) )
            {
                throw new CommandException( "Could not register secondary node." );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( commandDef.getDescription(), e );
            throw new MongoException( "Error on registering secondary node." );
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
}
