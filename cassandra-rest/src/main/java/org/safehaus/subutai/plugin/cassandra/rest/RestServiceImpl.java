package org.safehaus.subutai.plugin.cassandra.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.cassandra.api.Cassandra;
import org.safehaus.subutai.plugin.cassandra.api.CassandraClusterConfig;
import org.safehaus.subutai.plugin.cassandra.api.TrimmedCassandraClusterConfig;
import org.safehaus.subutai.plugin.common.api.NodeState;

import com.google.common.base.Preconditions;


public class RestServiceImpl implements RestService
{

    private Cassandra cassandraManager;
    private Tracker tracker;


    public Tracker getTracker(){
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Cassandra getCassandraManager()
    {
        return cassandraManager;
    }


    public void setCassandraManager( final Cassandra cassandraManager )
    {
        this.cassandraManager = cassandraManager;
    }


    @Override
    public Response listClusters()
    {
        List<CassandraClusterConfig> configs = cassandraManager.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( CassandraClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String source )
    {
        String cluster = JsonUtil.toJson( cassandraManager.getCluster( source ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response createCluster( final String config )
    {
        TrimmedCassandraClusterConfig trimmedCassandraConfig =
                JsonUtil.fromJson( config, TrimmedCassandraClusterConfig.class );

        CassandraClusterConfig cassandraConfig = new CassandraClusterConfig();
        cassandraConfig.setClusterName( trimmedCassandraConfig.getClusterName() );
        cassandraConfig.setDomainName( trimmedCassandraConfig.getDomainName() );
        cassandraConfig.setNumberOfNodes( trimmedCassandraConfig.getNumberOfNodes() );
        cassandraConfig.setNumberOfSeeds( trimmedCassandraConfig.getNumberOfSeeds() );

        UUID uuid = cassandraManager.installCluster( cassandraConfig );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        UUID uuid = cassandraManager.uninstallCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response configureCluster( final String environmentId, final String clusterName, final String nodes,
                                      final String seeds )
    {
        CassandraClusterConfig config = new CassandraClusterConfig();
        config.setEnvironmentId( UUID.fromString( environmentId ) );
        config.setClusterName( clusterName );
        Set<UUID> allNodes = new HashSet<>();
        Set<UUID> allSeeds = new HashSet<>();
        String[] configNodes = nodes.replaceAll( "\\s+", "" ).split( "," );
        String[] configSeeds = seeds.replaceAll( "\\s+", "" ).split( "," );
        for ( String node : configNodes )
        {
            allNodes.add( UUID.fromString( node ) );
        }
        for ( String node : configSeeds )
        {
            allSeeds.add( UUID.fromString( node ) );
        }
        config.setNodes( allNodes );
        config.setSeedNodes( allSeeds );

        UUID uuid = cassandraManager.installCluster( config );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        UUID uuid = cassandraManager.startCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        UUID uuid = cassandraManager.stopCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response addNode( final String clusterName, final String nodeType )
    {
        UUID uuid = cassandraManager.addNode( clusterName, nodeType );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @Override
    public Response destroyNode( final String clusterName, final String hostname )
    {
        UUID uuid = cassandraManager.destroyNode( clusterName, hostname );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response checkNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        UUID uuid = cassandraManager.checkNode( clusterName, hostname );

        TrackerOperationView po = tracker.getTrackerOperation( CassandraClusterConfig.PRODUCT_NAME, uuid );
        String operationId = wrapUUID( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        if ( state == OperationState.FAILED ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED ){
            return Response.status( Response.Status.OK ).entity( po.getLog() ).build();
        }
        else {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    private String wrapUUID( UUID uuid )
    {
        return JsonUtil.toJson( "OPERATION_ID", uuid );
    }


    private OperationState waitUntilOperationFinish( UUID uuid ){
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {

            TrackerOperationView po = tracker.getTrackerOperation( CassandraClusterConfig.PRODUCT_NAME, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    state = po.getState();
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }
        return state;
    }
}
