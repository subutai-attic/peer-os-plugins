package io.subutai.plugin.zookeeper.rest;


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
    // get container list
    @GET
    @Path( "containers/{envId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getContainers( @PathParam( "envId" ) String envId );


    //list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();

    //view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String source );

    //configure cluster
    @POST
    @Path( "configure_environment" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response configureCluster( @FormParam( "environmentId" ) String environmentId,
                                      @FormParam( "clusterName" ) String clusterName,
                                      @FormParam( "nodes" ) String nodes );


    //install cluster over hadoop
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @FormParam( "environmentId" ) String environmentId,
                                    @FormParam( "nodes" ) String nodes );

    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyCluster( @PathParam( "clusterName" ) String clusterName );

    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname );

    //start all nodes
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response startAllNodes( @PathParam( "clusterName" ) String clusterName );

    //check all nodes
    @GET
    @Path( "clusters/{clusterName}/check" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response checkAllNodes( @PathParam( "clusterName" ) String clusterName );

    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostname" ) String lxcHostname );


    //stop all nodes
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response stopAllNodes( @PathParam( "clusterName" ) String clusterName );


    //start nodes
    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response startNodes( @FormParam( "clusterName" ) String clusterName,
                                @FormParam( "lxcHostNames" ) String lxcHosts );

    //stop nodes
    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response stopNodes( @FormParam( "clusterName" ) String clusterName,
                               @FormParam( "lxcHostNames" ) String lxcHosts );


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostname" ) String lxcHostname );

    //check node status
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String lxcHostname );

    //add node over existing node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName,
                             @PathParam( "lxcHostname" ) String lxcHostname );

    //add node standalone
    @POST
    @Path( "clusters/{clusterName}/add/node" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response addNodeStandalone( @PathParam( "clusterName" ) String clusterName );

    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "scale" ) boolean scale );

    //get available nodes for adding
    @GET
    @Path( "clusters/{clusterName}/available/nodes" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response getAvailableNodes( @PathParam( "clusterName" ) String clusterName );


	@GET
	@Path( "about" )
	@Produces( { MediaType.TEXT_PLAIN } )
	public Response getPluginInfo();
}