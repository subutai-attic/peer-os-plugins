package io.subutai.plugin.cassandra.rest;


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
    @Path("clusters")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response listClusters();


    //view cluster info
    @GET
    @Path("clusters/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response getCluster( @PathParam("clusterName") String source );


    //destroy cluster
    @DELETE
    @Path("clusters/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyCluster( @PathParam("clusterName") String clusterName );


    //configure cluster
    @POST
    @Path( "configure_environment/{environmentId}/clusterName/{clusterName}/nodes/{nodes}/seeds/{seeds}" )
    @Produces( {MediaType.APPLICATION_JSON } )
    public Response configureCluster( @PathParam( "environmentId" ) String environmentId,
                                      @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "nodes" ) String nodes,
                                      @PathParam( "seeds" ) String seeds );


    //remove cluster
    @DELETE
    @Path("clusters/{clusterName}/remove")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response removeCluster( @PathParam("clusterName") String clusterName );


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


    //check cluster
    @GET
    @Path("clusters/{clusterName}/check")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkCluster( @PathParam("clusterName") String clusterName );


    //auto-scale cluster
    @POST
    @Path("clusters/{clusterName}/auto_scale/{scale}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response autoScaleCluster( @PathParam("clusterName") String clusterName,
                                      @PathParam( "scale" ) boolean scale );




    //add node
    @POST
    @Path("clusters/{clusterName}/add")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response addNode( @PathParam("clusterName") String clusterName );


    //destroy node
    @DELETE
    @Path("clusters/{clusterName}/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyNode( @PathParam("clusterName") String clusterName,
                                 @PathParam("lxcHostname") String lxcHostname );

    //check node status
    @GET
    @Path("clusters/{clusterName}/status/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );

    //start node
    @PUT
    @Path("clusters/{clusterName}/start/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response startNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );

    //stop node
    @PUT
    @Path("clusters/{clusterName}/stop/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response stopNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );
}