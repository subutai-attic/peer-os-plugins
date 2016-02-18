/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.rest;


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


/**
 *
 * @author caveman
 */
public interface RestService
{
    @GET
    @Path ( "clusters" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response listCluster ();


    @GET
    @Path ( "clusters/{clusterName}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response getCluster ( @PathParam ( "clusterName" ) String clusterName );


    @POST
    @Path ( "configure_environment" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response configureCluster ( @FormParam ( "clusterName" ) String clusterName );


    @DELETE
    @Path ( "clusters/{clusterName}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response uninstallCluster ( @PathParam ( "clusterName" ) String clusterName );


    @PUT
    @Path ( "clusters/{clusterName}/start" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response startNameNode ( @PathParam ( "clusterName" ) String clusterName );


    @PUT
    @Path ( "clusters/{clusterName}/stop" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response stopNameNode ( @PathParam ( "clusterName" ) String clusterName );


    @PUT
    @Path ( "clusters/{clusterName}/runssh" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response runSsh ( @PathParam ( "clusterName" ) String clusterName );
}

