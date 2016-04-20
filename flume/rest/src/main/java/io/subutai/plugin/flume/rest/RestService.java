package io.subutai.plugin.flume.rest;


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
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @FormParam( "nodes" ) String nodes );

    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    //add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostname" ) String node );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String node );


    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //check ndoe
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostname" ) String node );

    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response startNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHosts" ) String lxcHosts );

    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response stopNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHosts" ) String lxcHosts );

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
