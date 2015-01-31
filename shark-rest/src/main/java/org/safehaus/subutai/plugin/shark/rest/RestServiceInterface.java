package org.safehaus.subutai.plugin.shark.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public interface RestServiceInterface
{
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();

    @POST
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @QueryParam( "config" ) String config );

    @DELETE
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    @POST
    @Path( "clusters/{clusterName}/nodes/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName,
                             @PathParam( "lxcHostName" ) String lxcHostName );

    @DELETE
    @Path( "clusters/{clusterName}/nodes/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String lxcHostName );

    @PUT
    @Path( "clusters/{clusterName}/actualize_master_ip" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response actualizeMasterIP( @PathParam( "clusterName" ) String clusterName );
}
