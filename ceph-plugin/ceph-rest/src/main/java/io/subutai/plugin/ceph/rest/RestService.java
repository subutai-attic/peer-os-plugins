package io.subutai.plugin.ceph.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response installCluster( @FormParam( "environmentId" ) String environmentId,
                                    @FormParam( "lxcHostName" ) String lxcHostName,
                                    @FormParam( "clusterName" ) String clusterName);

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


    @GET
    @Path( "about" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getPluginInfo();
}
