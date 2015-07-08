package io.subutai.plugin.storm.rest;


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
                                    @QueryParam( "environmentId" ) String environmentId,
                                    @QueryParam( "externalZookeeper" ) boolean externalZookeeper,
                                    @QueryParam( "zookeeperClusterName" ) String zookeeperClusterName,
                                    @QueryParam( "nimbus" ) String nimbus,
                                    @QueryParam( "supervisors" ) String supervisorsCount );

    // destroy cluster
    @DELETE
    @Path( "clusters/destroy/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );

    // view cluster info
    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getCluster( @PathParam( "clusterName" ) String clusterName );

    // check node
    @GET
    @Path( "clusters/{clusterName}/check/node/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response statusCheck( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "hostname" ) String hostname );

    // start node
    @PUT
    @Path( "clusters/{clusterName}/start/node/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startNode( @PathParam( "clusterName" ) String clusterName,
                               @PathParam( "hostname" ) String hostname );

    // stop node
    @PUT
    @Path( "clusters/{clusterName}/stop/node/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopNode( @PathParam( "clusterName" ) String clusterName,
                              @PathParam( "hostname" ) String hostname );

    // add node
    @POST
    @Path( "clusters/{clusterName}/add" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response addNode( @PathParam( "clusterName" ) String clusterName );

    // destroy node
    @DELETE
    @Path( "clusters/{clusterName}/destroy/node/{hostname}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "hostname" ) String hostname );

    //check cluster
    @GET
    @Path( "clusters/{clusterName}/check" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response checkCluster( @PathParam( "clusterName" ) String clusterName );

    //start cluster
    @PUT
    @Path( "clusters/{clusterName}/start" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response startCluster( @PathParam( "clusterName" ) String clusterName );


    //stop cluster
    @PUT
    @Path( "clusters/{clusterName}/stop" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response stopCluster( @PathParam( "clusterName" ) String clusterName );


    //auto-scale cluster
    @POST
    @Path("clusters/{clusterName}/auto_scale/{scale}")
    @Produces({ MediaType.APPLICATION_JSON })
    public Response autoScaleCluster( @PathParam("clusterName") String clusterName,
                                      @PathParam( "scale" ) boolean scale );

}
