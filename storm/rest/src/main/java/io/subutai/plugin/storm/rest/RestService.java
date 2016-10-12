package io.subutai.plugin.storm.rest;


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
    // list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getClusters();

    // install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "environmentId" ) String environmentId,
                                    @FormParam( "nimbus" ) String nimbus,
                                    @FormParam( "supervisors" ) String supervisorsCount );

    // destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    // view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    // check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{nodeId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response statusCheck( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "nodeId" ) String nodeId );

    // start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{nodeId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "nodeId" ) String nodeId );

    // stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{nodeId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "nodeId" ) String nodeId );

    //start nodes
    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNodes( @FormParam( "clusterName" ) String clusterName,
                                @FormParam( "lxcHostIds" ) String lxcHostIds );

    //stop nodes
    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNodes( @FormParam( "clusterName" ) String clusterName,
                               @FormParam( "lxcHostIds" ) String lxcHostIds );

    // add node
    @POST
    @Path( "clusters/{clusterName}/add" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{nodeId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "nodeId" ) String nodeId );

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


    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "scale" ) boolean scale );


	@GET
	@Path( "about" )
	@Produces( { MediaType.TEXT_PLAIN } )
	public Response getPluginInfo();
}
