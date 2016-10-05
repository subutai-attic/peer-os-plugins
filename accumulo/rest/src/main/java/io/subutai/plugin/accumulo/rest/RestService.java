package io.subutai.plugin.accumulo.rest;


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
    Response installCluster( @FormParam( "clusterName" ) String clusterName,
                             @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                             @FormParam( "master" ) String master, @FormParam( "slaves" ) String slaves,
                             @FormParam( "pwd" ) String pwd );

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

    //start master node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{lxcHostId}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response startNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostId" ) String lxcHostName );


    //stop master node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{lxcHostId}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    Response stopNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostId" ) String lxcHostName );


    //check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{lxcHostName}/master/{master}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response checkNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostName" ) String lxcHostName,
                        @PathParam( "master" ) boolean master );

    @POST
    @Path( "clusters/nodes/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response startNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHostIds" ) String lxcHostIds );

    @POST
    @Path( "clusters/nodes/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response stopNodes( @FormParam( "clusterName" ) String clusterName, @FormParam( "lxcHostIds" ) String lxcHostIds );


    @GET
    @Path( "about" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response getPluginInfo();
}
