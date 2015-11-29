package io.subutai.plugin.presto.rest;


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


    //install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @FormParam( "master" ) String master, @FormParam( "workers" ) String workers );


    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    //add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addWorkerNode( @PathParam( "clusterName" ) String clusterName,
                                   @PathParam( "lxcHostName" ) String lxcHostName );


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyWorkerNode( @PathParam( "clusterName" ) String clusterName,
                                       @PathParam( "lxcHostName" ) String lxcHostName );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName );


	//start nodes
	@POST
	@Path("clusters/nodes/start")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response startNodes( @FormParam("clusterName") String clusterName,
								@FormParam("lxcHostNames") String lxcHosts );



    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostName" ) String lxcHostName );


	//stop nodes
	@POST
	@Path("clusters/nodes/stop")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response stopNodes( @FormParam("clusterName") String clusterName,
							   @FormParam("lxcHostNames") String lxcHosts );


    //check node
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


    //check cluster
    @GET
    @Path( "clusters/{clusterName}/check" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkCluster( @PathParam( "clusterName" ) String clusterName );

    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "scale" ) boolean scale );

    //get available nodes for adding
    @GET
    @Path( "clusters/{clusterName}/available/nodes" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getAvailableNodes( @PathParam( "clusterName" ) String clusterName );
}