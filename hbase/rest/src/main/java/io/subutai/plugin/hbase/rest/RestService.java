package io.subutai.plugin.hbase.rest;


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
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );


    //create cluster
    @POST
    @Path( "clusters" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response configureCluster( @FormParam( "config" ) String config );


    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyCluster( @PathParam( "clusterName" ) String clusterName );


    //start master node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                        @PathParam( "master" ) boolean master );


    //stop master node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                       @PathParam( "master" ) boolean master );


    //start region servers
    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHosts" ) String lxcHosts );


    //stop region servers
    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHosts" ) String lxcHosts );


    //add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName,
                             @PathParam( "lxcHostName" ) String lxcHostName );


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String lxcHostName );


    //check node status
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName );

    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "scale" ) boolean scale );


    //get available nodes for adding
    @GET
    @Path( "clusters/{clusterName}/available/nodes" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getAvailableNodes( @PathParam( "clusterName" ) String clusterName );

    @GET
    @Path( "about" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response getPluginInfo();

    @GET
    @Path( "angular" )
    Response getAngularConfig();
}