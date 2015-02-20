package org.safehaus.subutai.plugin.presto.rest;


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

    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();


    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );


    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @QueryParam( "clusterName" ) String clusterName,
                                    @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @QueryParam( "master" ) String master,
                                    @QueryParam( "workers" ) String workers );


    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addWorkerNode( @PathParam( "clusterName" ) String clusterName,
                                   @PathParam( "lxcHostName" ) String lxcHostName );


    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyWorkerNode( @PathParam( "clusterName" ) String clusterName,
                                       @PathParam( "lxcHostName" ) String lxcHostName );


    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName );


    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostName" ) String lxcHostName );


    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName );

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
}