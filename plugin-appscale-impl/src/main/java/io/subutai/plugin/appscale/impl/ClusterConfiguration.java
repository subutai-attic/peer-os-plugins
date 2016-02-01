/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


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

        // start executing commands
        // this.commandExecute ( containerHost, Commands.getRemoveSubutaiList () );
        this.commandExecute ( containerHost, Commands.getAddUbuntuUser () );
        this.commandExecute ( containerHost, Commands.getAddUserToRoot () );
        // this.commandExecute ( containerHost, Commands.getExportHome () );
        this.commandExecute ( containerHost, Commands.getFixLocale () );
        this.commandExecute ( containerHost, Commands.getCreateSshFolder () );
        this.commandExecute ( containerHost, Commands.getCreateAppscaleFolder () );
        this.commandExecute ( containerHost, Commands.getChangeRootPasswd () );

        List<String> zookeeperStopAndDisable = Commands.getZookeeperStopAndDisable ();
        for ( String c : zookeeperStopAndDisable )
        {
            this.commandExecute ( containerHost, c );
        }

        this.appscaleInitCluster ( containerHost ); // requires app.sh in container that is creating AppScalefile

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
            CommandResult responseFrom = containerHost.execute ( new RequestBuilder ( command ) );
            po.addLogDone ( command + " executed with: " + responseFrom );
        }
        catch ( CommandException e )
        {
            LOG.error ( "Error while executing \"" + command + "\".\n" + e );
        }
    }


    private void appscaleInitCluster ( EnvironmentContainerHost containerHost )
    {
        String ipaddr = getIPAddress ( containerHost );
        String localCommand = "sudo bash /home/ubuntu/app.sh " + ipaddr;
        try
        {
            CommandResult r = containerHost.execute ( new RequestBuilder ( localCommand ) );
            LOG.info ( ipaddr + " : " + r.getStdOut () + " " + r.getStdErr () );
        }
        catch ( Exception ex )
        {
            LOG.error ( "File can not be created." + ex );
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
            LOG.info ( "Container IP: " + ipaddr );
        }
        catch ( CommandException ex )
        {
            LOG.error ( "ip address command error : " + ex );
        }
        return ipaddr;

    }


}

