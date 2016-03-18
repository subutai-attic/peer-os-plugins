package io.subutai.plugin.backup.rest;


import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * Created by ermek on 2/24/16.
 */
public interface RestService
{
    // make backup
    @POST
    @Path( "container" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response executeBackup( @FormParam( "lxcHostName" ) String lxcHostName );
}
