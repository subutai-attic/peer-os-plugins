/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.rest;


import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.subutai.common.environment.Environment;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public interface RestService
{

    @GET
    @Path ( "clusters/{environmentID}" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response listClusterUI ( @PathParam ( "name" ) Environment environmentID );


    @GET
    @Path ( "clusterList" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response listClustersDB ();


    @POST
    @Path ( "configure_environment" )
    @Produces (
                        {
                MediaType.APPLICATION_JSON
            } )
    Response configureCluster ( @FormParam ( "clusterName" ) String clusterName, // this is tomcat template and single selection
                                @FormParam ( "userDomain" ) String userDomain, // user will enter the desired domain name
                                @FormParam ( "cassandraCSV" ) String cassandraCSV, // cassandra templates...
                                @FormParam ( "elasticSearchCSV" ) String elasticSearchCSV, // elastic search templates
                                @FormParam ( "environmentId" ) String environmentId ); // env.id will be send from ui
    /*
     * for each of the boxes only the related templates will be listed. for example if master (clustername) there will
     * be only tomcat templates and for cassandra only cassandra template will be listed for elastic only elasticsearch
     * templates will be listed
     */

}

