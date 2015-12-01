package io.subutai.plugin.generic.rest;


import javax.ws.rs.Consumes;
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

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.Multipart;


public interface RestService
{
    // get profiles list
    @GET
    @Path( "profiles" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listProfiles();

    // save profile
    @POST
    @Path( "profiles/create" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response saveProfile( @FormParam( "profileName" ) String profileName );

    // get profile operations list
    @GET
    @Path( "operations/{profileId}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    public Response listOperations( @PathParam( "profileId" ) String profileId );

    // save operation
    @POST
    @Path( "operations/script/create" )
    @Consumes( MediaType.MULTIPART_FORM_DATA )
    public Response saveOperation( @Multipart( "profileId" ) String profileId,
                                   @Multipart( "operationName" ) String operationName,
                                   @Multipart( value = "file" ) Attachment attr,
                                   @Multipart( "cwd" ) String cwd,
                                   @Multipart( "timeout" ) String timeout,
                                   @Multipart( "daemon" ) Boolean daemon,
                                   @Multipart( "script" ) Boolean script);

    // save operation
    @POST
    @Path( "operations/create" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response saveOperation( @FormParam( "profileId" ) String profileId,
                                   @FormParam( "operationName" ) String operationName,
                                   @FormParam( "commandName" ) String commandName,
                                   @FormParam( "cwd" ) String cwd,
                                   @FormParam( "timeout" ) String timeout,
                                   @FormParam( "daemon" ) Boolean daemon,
                                   @FormParam( "script" ) Boolean script);


    // update operation
    @POST
    @Path( "operations/update" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response updateOperation( @FormParam( "operationId" ) String operationId,
                                     @FormParam( "commandName" ) String commandName,
                                     @FormParam( "cwd" ) String cwd,
                                     @FormParam( "timeout" ) String timeout,
                                     @FormParam( "daemon" ) Boolean daemon,
                                     @FormParam( "script" ) Boolean script,
                                     @FormParam( "operationName" ) String operationName );


    // update operation
    @POST
    @Path( "operations/script/update" )
    @Consumes( MediaType.MULTIPART_FORM_DATA )
    public Response updateOperation( @Multipart( "operationId" ) String operationId,
                                     @Multipart( value = "file" ) Attachment attr,
                                     @Multipart( "cwd" ) String cwd,
                                     @Multipart( "timeout" ) String timeout,
                                     @Multipart( "daemon" ) Boolean daemon,
                                     @Multipart( "script" ) Boolean script,
                                     @Multipart( "operationName" ) String operationName );


    // delete operation
    @DELETE
    @Path( "operations/{operationId}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response deleteOperation( @PathParam( "operationId" ) String operationId );

    // delete profile
    @DELETE
    @Path( "profiles/{profileId}" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response deleteProfile( @PathParam( "profileId" ) String profileId );

    // execute operatoin on container
    @PUT
    @Path( "execute" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response executeCommand( @FormParam( "operationId" ) String operationId,
                                    @FormParam( "lxcHostName" ) String lxcHostName,
                                    @FormParam( "environmentId" ) String environmentId );
}

