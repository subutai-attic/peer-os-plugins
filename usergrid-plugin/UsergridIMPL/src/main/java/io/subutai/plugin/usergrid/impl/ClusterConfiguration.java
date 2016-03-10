/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.usergrid.api.UsergridConfig;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final TrackerOperation po;
    private final UsergridIMPL usergridImplManager;
    private final String catalinaHome = "/usr/share/tomcat7";
    private final String catalinaBase = "/var/lib/tomcat7";

    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterConfiguration ( final TrackerOperation operation, final UsergridIMPL usergridIMPL )
    {
        this.po = operation;
        this.usergridImplManager = usergridIMPL;
    }


    @Override
    public void configureCluster ( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        LOG.info ( "configureCluster: " );

        UsergridConfig config = ( UsergridConfig ) configBase;
        String tomcatName = config.getClusterName ();
        EnvironmentContainerHost tomcatContainerHost = null;
        try
        {
            tomcatContainerHost = environment.getContainerHostByHostname ( tomcatName );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "tomcat container host not found... " + ex );
        }

        List<String> cassandraNameList = config.getCassandraName ();
        List<String> elasticSearchList = config.getElasticSName ();

        // start command processes:
        this.commandExecute ( tomcatContainerHost, Commands.getExportJAVAHOME () );
        LOG.info ( "create properties file" );
        this.commandExecute ( tomcatContainerHost, Commands.getCreatePropertiesFile () );
        // start pushing properties file...
        this.pushToProperties ( tomcatContainerHost, "usergrid.cluster_name=" + tomcatName );
        this.pushToProperties ( tomcatContainerHost, "cassandra.cluster=" + cassandraNameList.get ( 0 ) );
        this.pushToProperties ( tomcatContainerHost, "cassandra.url=" + this.getIpCSV ( cassandraNameList, environment ) );
        this.pushToProperties ( tomcatContainerHost, "elasticsearch.cluster=" + elasticSearchList.get ( 0 ) );
        this.pushToProperties ( tomcatContainerHost, "elasticsearch.hosts=" + this.getIpCSV ( elasticSearchList,
                                                                                              environment ) );
        this.pushToProperties ( tomcatContainerHost, Commands.getAdminSuperUserString () );
        this.pushToProperties ( tomcatContainerHost, Commands.getAutoConfirmString () );
        this.pushToProperties ( tomcatContainerHost, "usergrid.api.url.base=http://" + this.getIPAddress (
                                tomcatContainerHost ) + ":8080/ROOT" );
        this.pushToProperties ( tomcatContainerHost, Commands.getBaseURL ().replace ( "${BASEURL}",
                                                                                      config.getUserDomain () ) );
        // end of pushing into file
        LOG.info ( "End of creating properties file" );
        this.commandExecute ( tomcatContainerHost,
                              "sudo cp /root/usergrid-deployment.properties " + catalinaHome + "/lib" );

        this.commandExecute ( tomcatContainerHost, Commands.getCopyRootWAR () );
        this.commandExecute ( tomcatContainerHost, Commands.getUntarPortal () );
        this.commandExecute ( tomcatContainerHost, Commands.getRenamePortal () );
        LOG.info ( "**************************************ALL DONE**************************************" );
        LOG.info ( "Restart TOMCAT7" );
        this.exportScriptCreate ( tomcatContainerHost );
        this.commandExecute ( tomcatContainerHost, "bash /exportScript.sh" ); // this restart tomcat as well..
        LOG.info ( "**************************************TOMCAT RESTARTED**************************************" );
        if ( !usergridImplManager.getPluginDAO ().saveInfo ( UsergridConfig.PRODUCT_NAME,
                                                             configBase.getClusterName (), configBase
        ) )
        {
            LOG.error ( "Usergrid can NOT be saved to DB" );
        }
        else
        {
            LOG.info ( "Usergrid SAVED to DB" );
        }

    }


    private void commandExecute ( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            CommandResult responseFrom = containerHost.execute ( new RequestBuilder ( command ).withTimeout ( 10000 ) );

        }
        catch ( CommandException e )
        {
            LOG.error ( "Error while executing \"" + command + "\".\n" + e );
        }
    }


    private String getIpCSV ( List<String> v, Environment environment )
    {
        List<String> ipList = new ArrayList ();

        v.stream ().forEach ( (cont)
                ->
                {
                    try
                    {
                        ipList.add ( this.getIPAddress ( environment.getContainerHostByHostname ( cont ) ) );
                    }
                    catch ( ContainerHostNotFoundException ex )
                    {
                        LOG.error ( "Container Not found while getting ip: " + ex );
                    }
        } );
        return String.join ( ",", ipList );
    }


    private void pushToProperties ( EnvironmentContainerHost containerHost, String value )
    {
        this.commandExecute ( containerHost,
                              "echo '" + value + "' >> /root/usergrid-deployment.properties" );
    }


    private String getIPAddress ( EnvironmentContainerHost ch )
    {
        String ipaddr = null;
        try
        {

            String localCommand = "ip addr | grep eth0 | grep \"inet\" | cut -d\" \" -f6 | cut -d\"/\" -f1";
            CommandResult resultAddr = ch.execute ( new RequestBuilder ( localCommand ) );
            ipaddr = resultAddr.getStdOut ();
            ipaddr = ipaddr.replace ( "\n", "" );
            LOG.info ( "Container IP: " + ipaddr );
        }
        catch ( CommandException ex )
        {
            LOG.error ( "ip address command error : " + ex );
        }
        return ipaddr;

    }


    private void exportScriptCreate ( EnvironmentContainerHost ch )
    {
        this.commandExecute ( ch, "rm exportScript.sh" );
        this.commandExecute ( ch, "touch exportScript.sh" );
        this.commandExecute ( ch, "echo '#!/bin/bash' >> exportScript.sh" );
        this.commandExecute ( ch, "echo export JAVA_HOME=\"/usr/lib/jvm/java-8-oracle\" >> exportScript.sh" );
        this.commandExecute ( ch, "echo 'sudo /etc/init.d/tomcat7 restart' >> exportScript.sh" );
        this.commandExecute ( ch, "chmod +x exportScript.sh" );
    }

}

