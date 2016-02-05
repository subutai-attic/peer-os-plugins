/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.common.api.ConfigBase;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final TrackerOperation po;
    private final AppScaleImpl appscaleManager;
    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterConfiguration ( final TrackerOperation operation, final AppScaleImpl appScaleImpl )
    {
        this.po = operation;
        this.appscaleManager = appScaleImpl;
    }


    /**
     *
     * @param configBase
     * @param environment
     * @throws ClusterConfigurationException
     *
     * configure cluster with appscale pre - requirements
     *
     * tutorial: https://confluence.subutai.io/display/APPSTALK/How+to+configure+container+for+the+needs+of+AppScale
     *
     */
    @Override
    public void configureCluster ( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        LOG.info ( "ClusterConfiguration :: configureCluster " );

        AppScaleConfig config = ( AppScaleConfig ) configBase;
        EnvironmentContainerHost containerHost = null;


        try
        {
            containerHost = environment.getContainerHostByHostname ( config.getClusterName () );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "configureCluster " + ex );
        }
        LOG.info (
                "Container Host Found: " + containerHost.getContainerId () + "\n"
                + "\n" + containerHost.getHostname () + "\n" );
        // start of commands
        this.commandExecute ( containerHost, Commands.getRemoveSubutaiList () );
        LOG.info ( "installing appscale can take 30 min or longer..." );
        po.addLog ( "installing appscale can take 30 min or longer..." );
        this.commandExecute ( containerHost, Commands.getAppscaleBuild () );
        LOG.info ( "installing appscale tools can take 30 min or longer..." );
        po.addLog ( "installing appscale tools can take 30 min or longer..." );
        this.commandExecute ( containerHost, Commands.getAppscaleToolsBuild () );
        this.appscaleInitCluster ( containerHost, environment, config );
        // end of executing commands

        config.setEnvironmentId ( environment.getId () );
        appscaleManager.getPluginDAO ().saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                                                   configBase );
        po.addLogDone ( "Appscale is saved to database" );
        this.commandExecute ( containerHost, Commands.getAppScaleStartCommand () );

    }


    private void commandExecute ( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            CommandResult responseFrom = containerHost.execute ( new RequestBuilder ( command ).withTimeout ( 4000 ) );
            po.addLogDone ( command + " executed with: " + responseFrom );
        }
        catch ( CommandException e )
        {
            LOG.error ( "Error while executing \"" + command + "\".\n" + e );
        }
    }


    /**
     *
     * @param containerHost
     * @param config
     *
     * we will use bash commands to create AppScale file
     */
    private void appscaleInitCluster ( EnvironmentContainerHost containerHost, Environment environment,
                                       AppScaleConfig config )
    {
        String ipaddr = getIPAddress ( containerHost );
        this.commandExecute ( containerHost, "rm -f /root/AppScalefile && touch /root/AppScalefile" );
        LOG.info ( "1" );
        this.commandExecute ( containerHost, "echo ips_layout: >> /root/AppScalefile" );
        LOG.info ( "2" );
        String cmdmaster = "echo '  master : " + ipaddr + "' >> /root/AppScalefile";
        this.commandExecute ( containerHost, cmdmaster );
        LOG.info ( "3" );
        this.commandExecute ( containerHost, "echo '  appengine : " + ipaddr + "' >> /root/AppScalefile" );
        if ( config.getZookeeperName () != null )
        {
            try
            {
                EnvironmentContainerHost zooContainerHost = environment.getContainerHostByHostname (
                        config.getZookeeperName () );
                String zooip = getIPAddress ( zooContainerHost );
                this.commandExecute ( containerHost,
                                      "echo '  zookeeper : " + zooip + "' >> /root/AppScalefile" );
                LOG.info ( "4" );

            }
            catch ( ContainerHostNotFoundException ex )
            {

            }
        }
        else
        {
            this.commandExecute ( containerHost, "echo '  zookeeper : " + ipaddr + "' >> /root/AppScalefile" );
            LOG.info ( "4" );
        }
        if ( config.getCassandraName () != null )
        {
            try
            {
                EnvironmentContainerHost cassContainerHost = environment.getContainerHostByHostname (
                        config.getCassandraName () );
                String cassIP = getIPAddress ( cassContainerHost );
                this.commandExecute ( containerHost,
                                      "echo '  database : " + cassIP + "' >> /root/AppScalefile" );
                LOG.info ( "4" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "Environment can not be found..." + ex );
            }
        }
        else
        {
            this.commandExecute ( containerHost, "echo '  database : " + ipaddr + "' >> /root/AppScalefile" );
            LOG.info ( "4" );
        }

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


}

