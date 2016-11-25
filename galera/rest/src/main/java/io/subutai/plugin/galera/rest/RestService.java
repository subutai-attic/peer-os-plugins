package io.subutai.plugin.galera.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
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

    //view cluster info
    @GET
    @Path( "clusters/{clustername}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clustername" ) String clustername );


    //install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response createCluster( @FormParam( "clusterName" ) String clusterName,
                                   @FormParam( "environmentId" ) String environmentId,
                                   @FormParam( "nodes" ) String nodes );

    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyCluster( @PathParam( "clusterName" ) String clusterName );


    // check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String node );

    //start nodes
    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNodes( @FormParam( "clusterName" ) String clusterName,
                                @FormParam( "lxcHosts" ) String lxcHosts );

    //stop nodes
    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNodes( @FormParam( "clusterName" ) String clusterName,
                               @FormParam( "lxcHosts" ) String lxcHosts );

    // add node
    @POST
    @Path( "clusters/{clusterName}/add" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String node );
}
