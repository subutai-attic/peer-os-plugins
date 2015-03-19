package org.safehaus.subutai.plugin.spark.rest;


import java.util.List;

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
    @Path( "clusters/install" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @QueryParam( "clusterName" ) String clusterName,
                                    @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @QueryParam( "master" ) String master,
                                    @QueryParam( "slaves" ) String workers );


    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    //add slave node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addSlaveNode( @PathParam( "clusterName" ) String clusterName,
                                  @PathParam( "lxcHostName" ) String lxcHostName );


    //destroy slave node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroySlaveNode( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "lxcHostName" ) String lxcHostName );


    //change master node
    @PUT
    @Path( "clusters/{clusterName}/change_master/nodes/{lxcHostName}/{keepSlave}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response changeMasterNode( @PathParam( "clusterName" ) String clusterName,
                                      @PathParam( "lxcHostName" ) String lxcHostName,
                                      @PathParam( "keepSlave" ) boolean keepSlave );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName, @PathParam( "master" ) boolean master );


    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "lxcHostName" ) String lxcHostName, @PathParam( "master" ) boolean master );


    //check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "lxcHostName" ) String lxcHostName, @PathParam( "master" ) boolean master );

    //start cluster
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startCluster( @PathParam( "clusterName" ) String clusterName);


    //stop cluster
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopCluster( @PathParam( "clusterName" ) String clusterName);

}