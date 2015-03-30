package org.safehaus.subutai.plugin.cassandra.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.cassandra.api.Cassandra;
import org.safehaus.subutai.plugin.cassandra.api.CassandraClusterConfig;
import org.safehaus.subutai.plugin.cassandra.api.TrimmedCassandraClusterConfig;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class RestServiceImpl implements RestService
{
    private Cassandra cassandraManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );


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
    public Response getCluster( final String clusterName )
    {
        CassandraClusterConfig config = cassandraManager.getCluster( clusterName );
        if ( config == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + " cluster not found." ).build();
        }
        String cluster = JsonUtil.toJson( config );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }



    @Override
    public Response destroyCluster( final String clusterName )
    {
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.uninstallCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response removeCluster( final String clusterName ){
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.removeCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }

    @Override
    public Response configureCluster( final String environmentId, final String clusterName, final String nodes,
                                      final String seeds )
    {
        Preconditions.checkNotNull( environmentId );
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodes );
        Preconditions.checkNotNull( seeds );
        Environment environment = null;
        try
        {
            environment = environmentManager.findEnvironment( UUID.fromString( environmentId  ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( environment == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "Could not find environment with id : " + environmentId ).build();
        }

        if ( cassandraManager.getCluster( clusterName ) != null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "There is already a cluster with same name !" ).build();
        }

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
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.startCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.stopCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.checkCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        CassandraClusterConfig config = cassandraManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            cassandraManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }


        return Response.status( Response.Status.OK ).entity( "Auto scale is set successfully" ).build();
    }


    @Override
    public Response addNode( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.addNode( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.destroyNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.checkNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found" ).build();
        }
        UUID uuid = cassandraManager.startService( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if( cassandraManager.getCluster( clusterName ) == null ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found" ).build();
        }
        UUID uuid = cassandraManager.stopService( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private Response createResponse( UUID uuid, OperationState state ){

            TrackerOperationView po = tracker.getTrackerOperation( CassandraClusterConfig.PRODUCT_NAME, uuid );


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
            if ( System.currentTimeMillis() - start > ( 90 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    public Tracker getTracker(){
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Cassandra getCassandraManager()
    {
        return cassandraManager;
    }

    public void setCassandraManager( final Cassandra cassandraManager )
    {
        this.cassandraManager = cassandraManager;
    }
}
