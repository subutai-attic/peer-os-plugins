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

import io.subutai.common.environment.Environment;


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
    Response listCluster ( @PathParam ( "name" ) Environment environmentID );


    @GET
    @Path ( "clusterList" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response listClusters ();


    @GET
    @Path ( "appscale" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response getIfAppscaleInstalled ( @PathParam ( "environmentID" ) Environment environmentID );


    @GET
    @Path ( "clusters/{clusterName}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response getCluster ( @PathParam ( "clusterName" ) String clusterName );


    @GET
    @Path ( "clusters/{clusterName}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response getConfigureSsh ( @PathParam ( "clusterName" ) String clusterName );


    @POST
    @Path ( "configure_environment" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response configureCluster ( @FormParam ( "clusterName" ) String clusterName,
                                @FormParam ( "zookeeperName" ) String zookeeperName,
                                @FormParam ( "cassandraName" ) String cassandraName,
                                @FormParam ( "envID" ) String envID );


    @DELETE
    @Path ( "clusters/{clusterName}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response uninstallCluster ( @PathParam ( "clusterName" ) String clusterName );


    @PUT
    @Path ( "clusters/{envID}/{operation}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response startStopMaster ( @PathParam ( "envID" ) Environment envID, String operation );


    @PUT
    @Path ( "clusters/{clusterName}/runssh" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response runSsh ( @PathParam ( "clusterName" ) String clusterName );
}

