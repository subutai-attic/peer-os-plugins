package io.subutai.plugin.spark.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
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
    @Path( "clusters" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response listClusters();


    //view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getCluster( @PathParam( "clusterName" ) String clusterName );


    //install cluster
    @POST
    @Produces( { MediaType.TEXT_PLAIN } )
    Response installCluster( @FormParam( "clusterName" ) String clusterName,
                             @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                             @FormParam( "master" ) String master, @FormParam( "slaves" ) String workers );


    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    //add slave node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response addSlaveNode( @PathParam( "clusterName" ) String clusterName,
                           @PathParam( "lxcHostName" ) String lxcHostName );


    //destroy slave node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response destroySlaveNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName );


    //change master node
    @PUT
    @Path( "clusters/{clusterName}/change_master/nodes/{lxcHostName}/{keepSlave}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response changeMasterNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName,
                               @PathParam( "keepSlave" ) boolean keepSlave );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                        @PathParam( "master" ) boolean master );


    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                       @PathParam( "master" ) boolean master );


    //check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                        @PathParam( "master" ) boolean master );

    //start cluster
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response startCluster( @PathParam( "clusterName" ) String clusterName );


    //stop cluster
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response stopCluster( @PathParam( "clusterName" ) String clusterName );


    //check cluster
    @GET
    @Path( "clusters/{clusterName}/check" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response checkCluster( @PathParam( "clusterName" ) String clusterName );
}