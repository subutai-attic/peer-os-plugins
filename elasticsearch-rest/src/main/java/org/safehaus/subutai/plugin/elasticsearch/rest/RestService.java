package org.safehaus.subutai.plugin.elasticsearch.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public interface RestService
{
    //list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();

    //configure cluster
    @POST
    @Path( "configure_environment/{environmentId}/clusterName/{clusterName}/nodes/{nodes}" )
    @Produces( {MediaType.APPLICATION_JSON } )
    public Response configureCluster( @PathParam( "environmentId" ) String environmentId,
                                      @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "nodes" ) String nodes );
    //remove cluster
    @DELETE
    @Path( "clusters/remove/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response removeCluster( @PathParam( "clusterName" ) String clusterName );

    //view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    //check cluster
    @GET
    @Path( "clusters/{clusterName}/check" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkCluster( @PathParam( "clusterName" ) String clusterName );

    //start cluster
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startCluster( @PathParam( "clusterName" ) String clusterName );


    //stop cluster
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopCluster( @PathParam( "clusterName" ) String clusterName );

    //check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );

    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );

    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );

    //add node
    @POST
    @Path( "clusters/{clusterName}/add" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName );

    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/remove/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );
}