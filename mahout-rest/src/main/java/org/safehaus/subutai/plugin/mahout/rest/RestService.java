package org.safehaus.subutai.plugin.mahout.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

    //create cluster
    @POST
    @Path("clusters/install")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response installCluster( @QueryParam( "clusterName" ) String clusterName,
                                   @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                   @QueryParam( "nodes" ) String nodes);

    //destroy cluster
    @DELETE
    @Path("clusters/remove/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyCluster( @PathParam("clusterName") String clusterName );

    //view cluster info
    @GET
    @Path("clusters/{clusterName}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response getCluster( @PathParam("clusterName") String clusterName );

    //check node status
    @GET
    @Path("clusters/{clusterName}/check/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response checkNode( @PathParam("clusterName") String clusterName,
                               @PathParam("lxcHostname") String lxcHostname );

    //add node
    @POST
    @Path("clusters/{clusterName}/add/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response addNode( @PathParam("clusterName") String clusterName,
                             @PathParam("lxcHostname") String lxcHostname );

    //destroy node
    @DELETE
    @Path("clusters/{clusterName}/remove/node/{lxcHostname}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response destroyNode( @PathParam("clusterName") String clusterName,
                                 @PathParam("lxcHostname") String lxcHostname );
}