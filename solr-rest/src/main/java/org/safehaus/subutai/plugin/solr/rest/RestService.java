package org.safehaus.subutai.plugin.solr.rest;


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


public interface RestService
{

    //list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();

    //install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response createCluster( @QueryParam( "clusterName" ) String clusterName,
                                   @QueryParam( "environmentId" ) String environmentId,
                                   @QueryParam( "nodes" ) String nodes );

    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyCluster( @PathParam( "clusterName" ) String clusterName );

    //view cluster info
    @GET
    @Path( "clusters/{clustername}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clustername" ) String clustername );

    //check node status
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname );

    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname );

    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostname" ) String lxcHostname );
}