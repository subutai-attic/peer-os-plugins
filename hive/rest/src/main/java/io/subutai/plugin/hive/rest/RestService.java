package io.subutai.plugin.hive.rest;


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


/**
 * Created by ermek on 2/19/15.
 */
public interface RestService
{
    // list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getClusters();

    //install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response installCluster( @FormParam( "clusterName" ) String clusterName,
                             @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                             @FormParam( "server" ) String server, @FormParam( "clients" ) String clients );

    // remove cluster
    @DELETE
    @Path( "clusters/remove/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    // view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getCluster( @PathParam( "clusterName" ) String clusterName );

    // check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );

    // start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );

    // stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );


    // add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/remove/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response destroyNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );


    //get available nodes for adding
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
