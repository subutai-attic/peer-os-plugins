package io.subutai.plugin.shark.rest;


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
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listClusters();

    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "sparkClusterName" ) String sparkClusterName );

    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName,
                             @PathParam( "lxcHostName" ) String lxcHostName );

    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String lxcHostName );

    @PUT
    @Path( "clusters/{clusterName}/actualize_master_ip" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response actualizeMasterIP( @PathParam( "clusterName" ) String clusterName );

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
