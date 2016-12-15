/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.network.ProxyLoadBalanceStrategy;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.protocol.ReverseProxyConfig;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.usergrid.api.UsergridConfig;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final TrackerOperation po;
    private final UsergridIMPL usergridImplManager;
    private final String catalinaHome = "/usr/share/tomcat7";


    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );


    public ClusterConfiguration( final TrackerOperation operation, final UsergridIMPL usergridIMPL )
    {
        this.po = operation;
        this.usergridImplManager = usergridIMPL;
    }


    @Override
    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        LOG.info( "configureCluster: " );

        UsergridConfig config = ( UsergridConfig ) configBase;
        String tomcatName = config.getClusterName();
        EnvironmentContainerHost tomcatContainerHost = null;
        try
        {
            tomcatContainerHost = environment.getContainerHostByHostname( tomcatName );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error( "tomcat container host not found... " + ex );
        }

        List<String> cassandraNameList = config.getCassandraName();

        cassandraNameList.stream().forEach( ( c ) ->
        {
            try
            {
                EnvironmentContainerHost cassContainerHost = environment.getContainerHostByHostname( c );
                this.commandExecute( cassContainerHost, Commands.replaceRPC() );
                this.commandExecute( cassContainerHost, Commands.getRestartCassandra() );
                this.commandExecute( cassContainerHost,
                        "echo '#!/bin/sh -e\n" + Commands.getRestartCassandra() + "\nexit 0' > /etc/rc.local" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error( "cassandra container host found error: " + ex );
            }
        } );
        List<String> elasticSearchList = config.getElasticSName();

        elasticSearchList.stream().forEach( ( e ) ->
        {
            try
            {
                EnvironmentContainerHost elContainerHost = environment.getContainerHostByHostname( e );
                this.commandExecute( elContainerHost, Commands.getStartElastic() );
                this.commandExecute( elContainerHost,
                        "echo '#!/bin/sh -e\n" + Commands.getStartElastic() + "\nexit 0' > /etc/rc.local" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error( ex.toString() );
            }
        } );

        // start command processes:
        this.commandExecute( tomcatContainerHost, Commands.getRemoveSourcesList() );
        this.commandExecute( tomcatContainerHost, Commands.getAptgetUpdate() );
        this.commandExecute( tomcatContainerHost, Commands.getInstallCurl() );

        LOG.info( "create properties file" );
        this.commandExecute( tomcatContainerHost, Commands.getCreatePropertiesFile() );
        // start pushing properties file...
        this.pushToProperties( tomcatContainerHost, "usergrid.cluster_name=" + tomcatName );
        this.pushToProperties( tomcatContainerHost, "cassandra.embedded=false" );
        this.pushToProperties( tomcatContainerHost, "cassandra.timeout=2000" );
        this.pushToProperties( tomcatContainerHost, Commands.getCollectionString() );
        this.pushToProperties( tomcatContainerHost, "cassandra.cluster=" + cassandraNameList.get( 0 ) );
        this.pushToProperties( tomcatContainerHost,
                "cassandra.url=" + this.getIpCSV( cassandraNameList, environment ) + ":9160" );
        this.pushToProperties( tomcatContainerHost, "elasticsearch.embedded=false" );
        this.pushToProperties( tomcatContainerHost, "elasticsearch.cluster=" + elasticSearchList.get( 0 ) );
        this.pushToProperties( tomcatContainerHost, "elasticsearch.index_prefix=usergrid" );
        this.pushToProperties( tomcatContainerHost,
                "elasticsearch.hosts=" + this.getIpCSV( elasticSearchList, environment ) );

        this.pushToProperties( tomcatContainerHost, "elasticsearch.force_refresh=true" );
        this.pushToProperties( tomcatContainerHost, "index.query.limit.default=100" );
        this.pushToProperties( tomcatContainerHost, Commands.getAdminSuperUserString() );
        this.pushToProperties( tomcatContainerHost, Commands.getAutoConfirmString() );

        this.pushToProperties( tomcatContainerHost, "usergrid.api.url.base=http://localhost:8080/ROOT" );
        this.pushToProperties( tomcatContainerHost,
                Commands.getBaseURL().replace( "${BASEURL}", "8080." + config.getUserDomain() ) );
        // end of pushing into file
        LOG.info( "End of creating properties file" );
        this.commandExecute( tomcatContainerHost,
                "sudo cp /root/usergrid-deployment.properties " + catalinaHome + "/lib" );
        this.configureReversProxy( tomcatContainerHost, config, tomcatName );
        this.commandExecute( tomcatContainerHost, Commands.getRemoveROOTFolder() );
        this.commandExecute( tomcatContainerHost, Commands.getCopyRootWAR() );
        this.commandExecute( tomcatContainerHost, Commands.getUntarPortal() );
        this.commandExecute( tomcatContainerHost, Commands.getRenamePortal() );
        // change urloverride...
        String commandToChange = "sed -i -e 's/localhost:8080/8080." + config.getUserDomain()
                + "/g' /var/lib/tomcat7/webapps/portal/config.js";
        this.commandExecute( tomcatContainerHost, commandToChange );

        this.commandExecute( tomcatContainerHost, Commands.makeSureApacheRestartOnBoot() );

        LOG.info( "**************************************ALL DONE**************************************" );
        LOG.info( "Restart TOMCAT7" );
        this.exportScriptCreate( tomcatContainerHost );
        this.commandExecute( tomcatContainerHost, "bash /exportScript.sh" ); // this restart tomcat as well..
        LOG.info( "**************************************TOMCAT RESTARTED**************************************" );

        try
        {
            LOG.info( "Wait to tomcat get alive" );
            TimeUnit.MINUTES.sleep( 1 );
            LOG.info( "completed" );
        }
        catch ( InterruptedException ex )
        {
            LOG.error( "error on waiting..." );
        }

        List<String> curlCommands = Commands.getCurlCommands();
        for ( String comm : curlCommands )
        {
            this.commandExecute( tomcatContainerHost, comm );
        }


        if ( !usergridImplManager.getPluginDAO()
                                 .saveInfo( UsergridConfig.PRODUCT_NAME, configBase.getClusterName(), configBase ) )
        {
            LOG.error( "Usergrid can NOT be saved to DB" );
        }
        else
        {
            LOG.info( "Usergrid SAVED to DB" );
        }
        LOG.info( "DONE" );
        po.addLogDone( "DONE" );
    }


    private void configureReversProxy( EnvironmentContainerHost containerHost, UsergridConfig config,
                                       String clusterName )
    {
        this.commandExecute( containerHost,
                "sed -i 's/127.0.0.1 localhost/127.0.0.1 localhost " + config.getUserDomain() + "/g' " + "/etc/hosts" );
        this.commandExecute( containerHost, "echo '" + Commands.get000Default( config.getUserDomain() )
                + "' > /etc/apache2/sites-enabled/000-default.conf" );
        this.commandExecute( containerHost, Commands.getCopyModes() );
        this.commandExecute( containerHost, Commands.getCopyXMLEnc() );
        this.commandExecute( containerHost, Commands.getCopySlotMem() );
        this.commandExecute( containerHost, Commands.getPutJAVAHome() );
        this.commandExecute( containerHost, "/etc/init.d/apache2 restart" );

        try
        {
            ReverseProxyConfig proxyConfig = new ReverseProxyConfig( config.getEnvironmentId(), containerHost.getId(),
                    "*." + config.getUserDomain(), "/mnt/lib/lxc/" + clusterName + "/rootfs/etc/nginx/ssl.pem",
                    ProxyLoadBalanceStrategy.NONE, 80 );

            containerHost.getPeer().addReverseProxy( proxyConfig );
        }
        catch ( Exception e )
        {
            LOG.error( "Error to set proxy settings: ", e );
        }
    }


    private void commandExecute( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            CommandResult responseFrom = containerHost.execute( new RequestBuilder( command ).withTimeout( 10000 ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error while executing \"" + command + "\".\n" + e );
        }
    }


    private String getIpCSV( List<String> v, Environment environment )
    {
        List<String> ipList = new ArrayList();

        v.stream().forEach( ( cont ) ->
        {
            try
            {
                ipList.add( environment.getContainerHostByHostname( cont ).getInterfaceByName( "eth0" ).getIp() );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error( "Container Not found while getting ip: " + ex );
            }
        } );
        return String.join( ",", ipList );
    }


    private void pushToProperties( EnvironmentContainerHost containerHost, String value )
    {
        this.commandExecute( containerHost, "echo '" + value + "' >> /root/usergrid-deployment.properties" );
    }


    private void exportScriptCreate( EnvironmentContainerHost ch )
    {
        this.commandExecute( ch, "rm exportScript.sh" );
        this.commandExecute( ch, "touch exportScript.sh" );
        this.commandExecute( ch, "echo '#!/bin/bash' >> exportScript.sh" );
        this.commandExecute( ch, "echo export JAVA_HOME=\"/usr/lib/jvm/java-8-oracle\" >> exportScript.sh" );

        // add curl commands here...
        this.commandExecute( ch, "echo '/etc/init.d/tomcat7 restart' >> exportScript.sh" );
        this.commandExecute( ch, "chmod +x exportScript.sh" );
    }
}

