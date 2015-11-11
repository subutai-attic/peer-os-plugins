package io.subutai.plugin.generic.rest;


import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 * Created by ermek on 11/10/15.
 */
public interface RestService
{
    // get profiles list
    @GET
    @Path( "profiles" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listProfiles();

    // save profile
    @POST
    @Path( "profiles/{profileName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response saveProfile( @PathParam( "profileName" ) String profileName );

    // get profile operations list
    @GET
    @Path( "operations/{profileId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listOperations( @PathParam( "profileId" ) String profileId );

    // save operation
    @POST
    @Path( "operations" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response saveOperation( @QueryParam( "profileId" ) String profileId,
                                   @QueryParam( "operationName" ) String operationName,
                                   @QueryParam( "commandName" ) String commandName, @QueryParam( "cwd" ) String cwd,
                                   @QueryParam( "timeout" ) String timeout, @QueryParam( "daemon" ) Boolean daemon,
                                   @QueryParam( "script" ) Boolean script );

    // update operation
    @POST
    @Path( "operations/update" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response updateOperation( @QueryParam( "operationId" ) String operationId,
                                     @QueryParam( "commandName" ) String commandName, @QueryParam( "cwd" ) String cwd,
                                     @QueryParam( "timeout" ) String timeout, @QueryParam( "daemon" ) Boolean daemon,
                                     @QueryParam( "script" ) Boolean script );

    // delete operation
    @DELETE
    @Path( "operations/{operationId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response deleteOperation( @PathParam( "operationId" ) String operationId );

    // delete profile
    @DELETE
    @Path( "profiles/{profileId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response deleteProfile( @PathParam( "profileId" ) String profileId );
}

