package org.safehaus.subutai.plugin.zookeeper.rest;


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

    //configure cluster
    @POST
    @Path("configure_environment/{environmentId}/clusterName/{clusterName}/nodes/{nodes}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response configureCluster( @PathParam("environmentId") String environmentId,
                                      @PathParam("clusterName") String clusterName, @PathParam("nodes") String nodes );


    //install cluster over hadoop
    @POST
    @Path("clusters/install")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response installCluster( @QueryParam("clusterName") String clusterName,
                                    @QueryParam("hadoopClusterName") String hadoopClusterName,
                                    @QueryParam("nodes") String nodes );

    //destroy cluster
    @DELETE
    @Path("clusters/destroy/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyCluster( @PathParam("clusterName") String clusterName );

    //start node
    @PUT
    @Path("clusters/{clusterName}/start/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response startNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );

    //start all nodes
    @PUT
    @Path("clusters/{clusterName}/start")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response startAllNodes( @PathParam("clusterName") String clusterName );

    //check all nodes
    @GET
    @Path("clusters/{clusterName}/check")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkAllNodes( @PathParam("clusterName") String clusterName );

    //stop node
    @PUT
    @Path("clusters/{clusterName}/stop/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response stopNode( @PathParam("clusterName") String clusterName,
                              @PathParam("lxcHostname") String lxcHostname );


    //stop all nodes
    @PUT
    @Path("clusters/{clusterName}/stop")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response stopAllNodes( @PathParam("clusterName") String clusterName );


    //destroy node
    @DELETE
    @Path("clusters/{clusterName}/destroy/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyNode( @PathParam("clusterName") String clusterName,
                                 @PathParam("lxcHostname") String lxcHostname );

    //check node status
    @GET
    @Path("clusters/{clusterName}/check/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );

    //add node over existing node
    @POST
    @Path("clusters/{clusterName}/add/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response addNode( @PathParam("clusterName") String clusterName,
                             @PathParam("lxcHostname") String lxcHostname );

    //add node standalone
    @POST
    @Path("clusters/{clusterName}/add/node")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response addNodeStandalone( @PathParam("clusterName") String clusterName );

    //auto-scale cluster
    @POST
    @Path("clusters/{clusterName}/auto_scale/{scale}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response autoScaleCluster( @PathParam("clusterName") String clusterName,
                                      @PathParam( "scale" ) boolean scale );
}