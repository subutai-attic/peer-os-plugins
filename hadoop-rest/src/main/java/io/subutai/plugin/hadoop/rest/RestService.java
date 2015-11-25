package io.subutai.plugin.hadoop.rest;


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
    Response configureCluster( @QueryParam( "config" ) String config );

    //configure cluster
    @POST
    @Path( "configure_environment" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response configureCluster( @QueryParam( "environmentId" ) String environmentId,
                               @QueryParam( "clusterName" ) String clusterName,
                               @QueryParam( "nameNode" ) String nameNode,
                               @QueryParam( "secNameNode" ) String secNameNode,
                               @QueryParam( "jobTracker" ) String jobTracker,
                               @QueryParam( "slaves" ) String slaves,
                               @QueryParam( "replicationFactor" ) int replicationFactor);


    //uninstall cluster
    @DELETE
    @Path( "clusters/{clusterName}" ) //Maps for the `hello/John` in the URL
    @Produces( { MediaType.APPLICATION_JSON } )
    Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    //startNameNode
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startNameNode( @PathParam( "clusterName" ) String clusterName );

    //stopNameNode
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopNameNode( @PathParam( "clusterName" ) String clusterName );

    //statusNameNode
    @GET
    @Path( "clusters/{clusterName}/status" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response statusNameNode( @PathParam( "clusterName" ) String clusterName );

    //statusSecondaryNameNode
    @GET
    @Path( "clusters/{clusterName}/status/secondary" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response statusSecondaryNameNode( @PathParam( "clusterName" ) String clusterName );

    //startJobTracker
    @PUT
    @Path( "clusters/job/{clusterName}/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startJobTracker( @PathParam( "clusterName" ) String clusterName );

    //stopJobTracker
    @PUT
    @Path( "clusters/job/{clusterName}/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopJobTracker( @PathParam( "clusterName" ) String clusterName );

    //statusJobTracker
    @GET
    @Path( "clusters/job/{clusterName}/status" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response statusJobTracker( @PathParam( "clusterName" ) String clusterName );

    //addNode
    @POST
    @Path( "clusters/{clusterName}/nodes" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response addNode( @PathParam( "clusterName" ) String clusterName );

    //statusDataNode
    @GET
    @Path( "clusters/{clusterName}/node/{hostname}/status" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response statusDataNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "hostname" ) String hostname );

    //statusTaskTracker
    @GET
    @Path( "clusters/{clusterName}/task/{hostname}/status" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response statusTaskTracker( @PathParam( "clusterName" ) String clusterName,
                                @PathParam( "hostname" ) String hostname );

    //auto-scale cluster
    @POST
    @Path( "clusters/{clusterName}/auto_scale/{scale}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response autoScaleCluster( @PathParam( "clusterName" ) String clusterName, @PathParam( "scale" ) boolean scale );
}