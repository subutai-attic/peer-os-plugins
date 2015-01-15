package org.safehaus.subutai.plugin.hive.rest;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.plugin.hive.api.Hive;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;

import com.google.common.collect.Sets;


public class RestService
{
    private static final String OPERATION_ID = "OPERATION_ID";
    private Hive hiveManager;


    public RestService( final Hive hiveManager )
    {
        this.hiveManager = hiveManager;
    }


    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getClusters()
    {

        List<HiveConfig> configs = hiveManager.getClusters();
        Set<String> clusterNames = Sets.newHashSet();

        for ( HiveConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName )
    {
        HiveConfig config = hiveManager.getCluster( clusterName );
        String cluster = JsonUtil.GSON.toJson( config );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @POST
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @PathParam( "clusterName" ) String clusterName,
                                    @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @QueryParam( "server" ) String server, @QueryParam( "clients" ) String clients )
    {

        HiveConfig config = new HiveConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );

        config.setServer( UUID.fromString( server ) );

        for ( String client : clients.split( "," ) )
        {
            config.getClients().add( UUID.fromString( client ) );
        }

        UUID uuid = hiveManager.installCluster( config );

        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @DELETE
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName )
    {
        UUID uuid = hiveManager.uninstallCluster( clusterName );
        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @POST
    @Path( "clusters/{clusterName}/nodes/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "hostname" ) String hostname )
    {
        UUID uuid = hiveManager.addNode( clusterName, hostname );
        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @DELETE
    @Path( "clusters/{clusterName}/nodes/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "hostname" ) String hostname )
    {
        UUID uuid = hiveManager.uninstallNode( clusterName, hostname );

        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }
}
