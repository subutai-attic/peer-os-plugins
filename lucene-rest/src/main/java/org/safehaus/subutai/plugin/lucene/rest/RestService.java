package org.safehaus.subutai.plugin.lucene.rest;



import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;

public interface RestService {

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
}
