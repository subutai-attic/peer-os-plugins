package org.safehaus.subutai.plugin.flume.rest;


import java.util.List;
import java.util.UUID;

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
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response installCluster( @PathParam( "clusterName" ) String clusterName,
                                    @QueryParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @QueryParam( "nodes" ) String nodes );

    //destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    //add node
    @POST
    @Path( "clusters/{clusterName}/add/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //check ndoe
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );
}
