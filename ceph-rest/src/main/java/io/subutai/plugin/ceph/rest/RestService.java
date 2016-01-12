package io.subutai.plugin.ceph.rest;


import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


public interface RestService
{
    // configure cluster
    @POST
    @Path( "configure" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response configureCluster( @FormParam( "environmentId" ) String environmentId,
                                      @FormParam( "lxcHostName" ) String lxcHostName,
                                      @FormParam( "clusterName" ) String clusterName);
}
