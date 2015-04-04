package org.safehaus.subutai.plugin.mongodb.impl;


import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoException;
import org.safehaus.subutai.plugin.mongodb.impl.common.CommandDef;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
            for ( UUID uuid : config.getDataHosts() )
            {
                ContainerHost dataNode = findContainerHost( uuid, environment );
                po.addLog( "Setting replicaSetname: " + dataNode.getHostname() );
                setReplicaSetName( dataNode, config.getReplicaSetName() );
            }

            ContainerHost datanode;
            for ( UUID uui : config.getDataHosts() )
            {
                datanode = findContainerHost( uui, environment );
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

        //subscribe to alerts
//        try
//        {
//            mongoManager.subscribeToAlerts( environment );
//        }
//        catch ( MonitorException e )
//        {
//            throw new ClusterConfigurationException( e );
//        }
    }


    public void setReplicaSetName( ContainerHost host, final String replicaSetName ) throws MongoException
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


    public void initiateReplicaSet( ContainerHost host, MongoClusterConfig config ) throws MongoException
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

    public void registerSecondaryNode( final ContainerHost dataNode, MongoClusterConfig config  ) throws MongoException
    {
        CommandDef commandDef =
                Commands.getRegisterSecondaryNodeWithPrimaryCommandLine( dataNode.getHostname(), config.getDataNodePort(), config.getDomainName() );
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


    public ContainerHost findContainerHost( UUID uuid, Environment environment ){
        try
        {
            return environment.getContainerHostById( uuid );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }

}
