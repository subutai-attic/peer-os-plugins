package io.subutai.plugin.mysql.rest;


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


/**
 * Created by tkila on 5/22/15.
 */
public interface RestService
{

    //list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();


    //view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );


    //configure cluster
    @POST
    @Path( "configure_environment" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response configureCluster( @QueryParam( "config" ) String config );


    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyCluster( @PathParam( "clusterName" ) String clusterName );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}/nodeType/{nodeType}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname,
                               @PathParam( "nodeType" ) String nodeType );


    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}/nodeType/{nodeType}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostname" ) String lxcHostname,
                              @PathParam( "nodeType" ) String nodeType );


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


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostname}/nodeType/{nodeType}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostname" ) String lxcHostname,
                                 @PathParam( "nodeType" ) String nodeType );


    //check node status
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}/nodeType/{nodeType}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname,
                               @PathParam( "nodeType" ) String nodeType );


    //add node
    @POST
    @Path( "clusters/{clusterName}/add/node/nodeType/{nodeType}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "nodeType" ) String nodeType );
}
