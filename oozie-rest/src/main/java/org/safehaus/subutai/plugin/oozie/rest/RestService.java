package org.safehaus.subutai.plugin.oozie.rest;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public interface RestService
{

    @GET
    @Path("clusters")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getClusters();

    @GET
    @Path("clusters/{clusterName}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getCluster(@PathParam("clusterName") String clusterName);

    @POST
    @Path("clusters/{clusterName}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response installCluster(@PathParam("clusterName") String clusterName,
                                   @QueryParam("nodes") String nodes);

    @POST
    @Path("install/{name}/{hadoopName}/{slaveNodesCount}/{replFactor}/{domainName}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response install(@PathParam("name") String name, @PathParam("hadoopName") String hadoopName,
                            @PathParam("slaveNodesCount") String slaveNodesCount,
                            @PathParam("replFactor") String replFactor,
                            @PathParam("domainName") String domainName);

    @DELETE
    @Path("clusters/{clusterName}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response uninstallCluster(@PathParam("clusterName") String clusterName);

    @POST
    @Path("clusters/{clusterName}/nodes/{node}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response addNode(@PathParam("clusterName") String clusterName, @PathParam("node") String node);

    @DELETE
    @Path("clusters/{clusterName}/nodes/{node}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response destroyNode(@PathParam("clusterName") String clusterName, @PathParam("node") String node);

    @PUT
    @Path("clusters/{clusterName}/nodes/{node}/start")
    @Produces({MediaType.APPLICATION_JSON})
    public Response startNode(@PathParam("clusterName") String clusterName, @PathParam("node") String node);

    @PUT
    @Path("clusters/{clusterName}/nodes/{node}/stop")
    @Produces({MediaType.APPLICATION_JSON})
    public Response stopNode(@PathParam("clusterName") String clusterName, @PathParam("node") String node);

    @GET
    @Path( "clusters/{clusterName}/nodes/{node}/check" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "node" ) String node );


//    //list clusters
//    @GET
//    @Path("clusters")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response listClusters();
//
//    //view cluster info
//    @GET
//    @Path("clusters/{clusterName}")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response getCluster( @PathParam("clusterName") String clusterName );
//
//    //create cluster
//    @POST
//    @Path("clusters")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response createCluster( @QueryParam("config") String config );
//
//    //destroy cluster
//    @DELETE
//    @Path("clusters/{clusterName}")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response destroyCluster( @PathParam("clusterName") String clusterName );
//
//    //start cluster
//    @PUT
//    @Path("clusters/{clusterName}/start")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response startCluster( @PathParam("clusterName") String clusterName );
//
//    //stop cluster
//    @PUT
//    @Path("clusters/{clusterName}/stop")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response stopCluster( @PathParam("clusterName") String clusterName );
//
//    //add node
//    @POST
//    @Path("clusters/{clusterName}/nodes/{lxcHostname}/{nodeType}")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response addNode( @PathParam("clusterName") String clusterName, @PathParam("lxcHostname") String lxcHostname,
//                             @PathParam("nodeType") String nodeType );
//
//    //destroy node
//    @DELETE
//    @Path("clusters/{clusterName}/nodes/{lxcHostname}/{nodeType}")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response destroyNode( @PathParam("clusterName") String clusterName,
//                                 @PathParam("lxcHostname") String lxcHostname );
//
//    //check node status
//    @GET
//    @Path("clusters/{clusterName}/nodes/{lxcHostname}")
//    @Produces({ MediaType.APPLICATION_JSON })
//    public Response checkNode( @PathParam("clusterName") String clusterName,
//                               @PathParam("lxcHostname") String lxcHostname );
}