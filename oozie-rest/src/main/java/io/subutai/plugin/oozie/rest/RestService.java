package io.subutai.plugin.oozie.rest;


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
    // list clusters
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getClusters();

    // install cluster
    @POST
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @QueryParam( "clusterName" ) String clusterName,
                                    @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @QueryParam( "server" ) String server, @QueryParam( "clients" ) String clients );

    // remove cluster
    @DELETE
    @Path( "clusters/remove/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    // view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    // check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String node );


    // start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String node );

    // stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );

    // add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String node );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/remove/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String node );
}