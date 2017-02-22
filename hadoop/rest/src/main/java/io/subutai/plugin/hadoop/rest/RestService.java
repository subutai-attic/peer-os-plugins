package io.subutai.plugin.hadoop.rest;


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
    // get container list
    @GET
    @Path( "containers/{envId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getContainers( @PathParam( "envId" ) String envId );


    // get cluster list
    @GET
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response listClusters();

    //view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getCluster( @PathParam( "clusterName" ) String clusterName );


    //configure cluster
    @POST
    @Path( "configure_environment" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response configureCluster( @FormParam( "config" ) String config );


    //uninstall cluster
    @DELETE
    @Path( "clusters/{clusterName}" ) //Maps for the `hello/John` in the URL
    @Produces( { MediaType.APPLICATION_JSON } )
    Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    //startNameNode
    @PUT
    @Path( "clusters/{clusterName}/start/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startNameNode( @PathParam( "clusterName" ) String clusterName,
                            @PathParam( "lxcHostName" ) String lxcHostName );

    //stopNameNode
    @PUT
    @Path( "clusters/{clusterName}/stop/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopNameNode( @PathParam( "clusterName" ) String clusterName,
                           @PathParam( "lxcHostName" ) String lxcHostName );


    //addNode
    @POST
    @Path( "clusters/{clusterName}/nodes" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response addNode( @PathParam( "clusterName" ) String clusterName );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/remove/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostName" ) String node );


    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName, @PathParam( "scale" ) boolean scale );


    @GET
    @Path( "about" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response getPluginInfo();


    @GET
    @Path( "angular" )
    Response getAngularConfig();
}