package org.safehaus.subutai.plugin.mongodb.impl;


import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoException;
import org.safehaus.subutai.plugin.mongodb.impl.common.CommandDef;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;


public class ClusterConfiguration
{

    private TrackerOperation po;
    private MongoImpl mongoManager;


    public ClusterConfiguration( final TrackerOperation operation, final MongoImpl mongoManager )
    {
        this.po = operation;
        this.mongoManager = mongoManager;
    }


    public void configureCluster( MongoClusterConfig config, Environment environment )
            throws ClusterConfigurationException
    {

        po.addLog( String.format( "Configuring cluster: %s", config.getClusterName() ) );

        // TODO : run configuration commands here

        try
        {
            //            for ( MongoConfigNode configNode : config.getConfigServers() )
            //            {
            //                po.addLog( "Starting config node: " + configNode.getHostname() );
            //                configNode.start( config );
            //            }

            for ( UUID uuid : config.getRouterHosts() ){
                setReplicaSetName( findContainerHost( uuid, environment ), config.getConfigHosts() );
            }
            for ( MongoRouterNode routerNode : config.getRouterServers() )
            {
                //                po.addLog( "Starting router node: " + routerNode.getHostname() );
                routerNode.setConfigServers( config.getConfigServers() );
                //                routerNode.start( config );
            }

            for ( MongoDataNode dataNode : config.getDataHosts() )
            {
                po.addLog( "Setting replicaSetname: " + dataNode.getHostname() );
                dataNode.setReplicaSetName( config.getReplicaSetName() );
            }


            //            for ( MongoDataNode dataNode : config.getDataHosts() )
            //            {
            //                po.addLog( "Stopping data node: " + dataNode.getHostname() );
            //                dataNode.stop();
            //            }

            MongoDataNode primaryDataNode = null;
            for ( MongoDataNode dataNode : config.getDataHosts() )
            {
                //                po.addLog( "Starting data node: " + dataNode.getHostname() );
                //                dataNode.start( config );
                if ( primaryDataNode == null )
                {
                    primaryDataNode = dataNode;
                    config.setPrimaryNode( primaryDataNode.getHostname() );
                    primaryDataNode.initiateReplicaSet();
                    po.addLog( "Primary data node: " + dataNode.getHostname() );
                }
                else
                {
                    po.addLog( "registering secondary data node: " + dataNode.getHostname() );
                    primaryDataNode.registerSecondaryNode( dataNode );
                }
            }
        }
        catch ( MongoException e )
        {
            e.printStackTrace();
            throw new ClusterConfigurationException( e );
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

        po.addLogDone( "Cassandra cluster data saved into database" );

        //subscribe to alerts
        try
        {
            mongoManager.subscribeToAlerts( environment );
        }
        catch ( MonitorException e )
        {
            throw new ClusterConfigurationException( e );
        }
    }


    public void setReplicaSetName( ContainerHost host, final String replicaSetName ) throws MongoException
    {
        try
        {
            CommandDef commandDef = Commands.getSetReplicaSetNameCommandLine( replicaSetName );
            CommandResult commandResult = host.execute( commandDef.build().withTimeout( 90 ) );
        }
        catch ( CommandException e )
        {
            throw new MongoException( "Error on setting replica set name: " );
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
