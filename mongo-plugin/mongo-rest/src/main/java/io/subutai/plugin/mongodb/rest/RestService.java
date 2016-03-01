package io.subutai.plugin.mongodb.rest;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public interface RestService
{

    //list clusters
    @GET
    @Path("clusters")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response listClusters();

    //view cluster info
    @GET
    @Path("clusters/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response getCluster( @PathParam("clusterName") String clusterName );


    //configure cluster
    @POST
    @Path("configure_environment")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response configureCluster( @QueryParam("config") String config );

    //destroy cluster
    @DELETE
    @Path("clusters/destroy/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyCluster( @PathParam("clusterName") String clusterName );

    //start node
    @PUT
    @Path("clusters/{clusterName}/start/node/{lxcHostname}/nodeType/{nodeType}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response startNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname,
                               @PathParam("nodeType") String nodeType );


	//start nodes
	@POST
	@Path("clusters/nodes/start")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response startNodes( @FormParam("clusterName") String clusterName,
								@FormParam("lxcHostNames") String lxcHosts );

    //stop node
    @PUT
    @Path("clusters/{clusterName}/stop/node/{lxcHostname}/nodeType/{nodeType}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response stopNode( @PathParam("clusterName") String clusterName,
                              @PathParam("lxcHostname") String lxcHostname,
                              @PathParam("nodeType") String nodeType );


	//stop nodes
	@POST
	@Path("clusters/nodes/stop")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response stopNodes( @FormParam("clusterName") String clusterName,
							   @FormParam("lxcHostNames") String lxcHosts );

    //start cluster
    @PUT
    @Path("clusters/{clusterName}/start")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response startCluster( @PathParam("clusterName") String clusterName );


    //stop cluster
    @PUT
    @Path("clusters/{clusterName}/stop")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response stopCluster( @PathParam("clusterName") String clusterName );


    //destroy node
    @DELETE
    @Path("clusters/{clusterName}/destroy/node/{lxcHostname}/nodeType/{nodeType}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyNode( @PathParam("clusterName") String clusterName,
                                 @PathParam("lxcHostname") String lxcHostname,
                                 @PathParam("nodeType") String nodeType );

    //check node status
    @GET
    @Path("clusters/{clusterName}/check/node/{lxcHostname}/nodeType/{nodeType}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname,
                               @PathParam("nodeType") String nodeType );

    //add node
    @POST
    @Path("clusters/{clusterName}/add/node/nodeType/{nodeType}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response addNode( @PathParam("clusterName") String clusterName, @PathParam("nodeType") String nodeType );


    // auto-scale
    @POST
    @Path ("clusters/{clusterName}/auto_scale/{scale}")
    @Produces ({MediaType.APPLICATION_JSON})
	public Response autoScaleCluster( @PathParam("clusterName") String clusterName,
									  @PathParam( "scale" ) boolean scale );

	@POST
	@Path( "clusters/create" )
	@Produces({MediaType.APPLICATION_JSON})
	Response installCluster( @FormParam ("clusterConfJson") String config );

}