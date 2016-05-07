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
 * @author caveman
 */
public interface RestService
{
    @GET
    @Path( "clusters/{environmentID}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response listCluster( @PathParam( "name" ) Environment environmentID );


    @GET
    @Path( "clusterList" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response listClusters();


    @GET
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response getCluster( @PathParam( "clusterName" ) String clusterName );

    @POST
    @Path( "configure_environment" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response installCluster( @FormParam( "clusterName" ) String clusterName,
                               @FormParam( "appengineName" ) String appengine,
                               @FormParam( "zookeeperName" ) String zookeeperName,
                               @FormParam( "cassandraName" ) String cassandraName,
                               @FormParam( "envID" ) String envID,
                               @FormParam( "userDomain" ) String userDomain,
                               @FormParam( "login" ) String login,
                               @FormParam( "password" ) String password );


    @DELETE
    @Path( "clusters/{clusterName}" )
    @Produces( { MediaType.APPLICATION_JSON } )
    Response uninstallCluster( @PathParam( "clusterName" ) String clusterName );


    @GET
    @Path( "angular" )
    Response getAngularConfig();


    @GET
    @Path( "about" )
    @Produces( { MediaType.TEXT_PLAIN } )
    public Response getPluginInfo();
}
