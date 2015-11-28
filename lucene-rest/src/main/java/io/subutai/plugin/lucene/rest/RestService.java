package io.subutai.plugin.lucene.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response installCluster( @FormParam( "clusterName" ) String clusterName,
                                    @FormParam( "hadoopClusterName" ) String hadoopClusterName,
                                    @FormParam( "nodes" ) String nodes );


    //destroy cluster
    @DELETE
    @Produces( { MediaType.TEXT_PLAIN } )
    @Path( "clusters/destroy/{clusterName}" )
    public Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    //add node
    @POST
    @Produces( { MediaType.TEXT_PLAIN } )
    @Path( "clusters/{clusterName}/add/node/{lxcHostname}" )
    public Response addNode( @PathParam( "clusterName" ) String clusterName, @PathParam( "lxcHostname" ) String node );


    //destroy node
    @DELETE
    @Produces( { MediaType.TEXT_PLAIN } )
    @Path( "clusters/{clusterName}/destroy/node/{lxcHostname}" )
    public Response destroyNode( @PathParam( "clusterName" ) String clusterName,
                                 @PathParam( "lxcHostname" ) String node );


    //get available nodes for adding
    @GET
    @Path( "clusters/{clusterName}/available/nodes" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response getAvailableNodes( @PathParam( "clusterName" ) String clusterName );
}
