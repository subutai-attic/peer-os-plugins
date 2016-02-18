/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.peer.api.PeerManager;
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
        System.setProperty ( "user.dir", "/root" );
        LOG.info ( "we are running this in : " + System.getProperty ( "user.dir" ) );

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

        // this.commandExecute ( containerHost, Commands.getRemoveSubutaiList () );
        this.commandExecute ( containerHost, Commands.getCreateLogDir () );

        LOG.info ( "installing appscale can take several minutes." );
        po.addLog ( "installing appscale can take several minutes." );

        // AppScalefile configuration
        this.appscaleInitCluster ( containerHost, environment, config );
        // end of AppScalefile configuration
        LOG.info ( "cleaning up..." );
        this.makeCleanUpPreviousInstallation ( containerHost );
        LOG.info ( "clean up ended..." );
        LOG.info ( "Opening necessary PORTS" );
        // this.runShhInResourceHost ( containerHost ); // this bit has to change..
        LOG.info ( "opening the ports finished" );
        LOG.info ( "Run shell starting..." );
        this.commandExecute ( containerHost, Commands.getRunShell () );
        LOG.info ( "Run shell completed..." );
        LOG.info ( "Correcting Hostname" );
        // this.commandExecute ( containerHost, "sudo echo '" + config.getClusterName () + "' > /etc/hostname" );


        LOG.info ( "Login into your RH and run these commands: \n"
                + "ssh -f -N -R 1443:<Your RH IP>:1443 ubuntu@localhost\n"
                + "ssh -f -N -R 5555:<Your RH IP>:5555 ubuntu@localhost\n"
                + "ssh -f -N -R 8081:<Your RH IP>:8081 ubuntu@localhost\n" );
        po.addLog ( "Login into your RH and run these commands: \n"
                + "ssh -f -N -R 1443:<Your RH IP>:1443 ubuntu@localhost\n"
                + "ssh -f -N -R 5555:<Your RH IP>:5555 ubuntu@localhost\n"
                + "ssh -f -N -R 8081:<Your RH IP>:8081 ubuntu@localhost\n" );

        config.setEnvironmentId ( environment.getId () );
        appscaleManager.getPluginDAO ().saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                                                   configBase );
        LOG.info ( "Appscale saved to database" );
        po.addLogDone ( "Appscale is saved to database" );


    }


    public void runShhInResourceHost ( EnvironmentContainerHost containerHost )
    {
        PeerManager peerManager = appscaleManager.getPeerManager ();
        LocalPeer localPeer = peerManager.getLocalPeer ();
        String ipAddress = this.getIPAddress ( containerHost ); // only for master
        String command1443 = "ssh -f -N -R 1443:" + ipAddress + ":1443 ubuntu@localhost";
        String command5555 = "ssh -f -N -R 5555:" + ipAddress + ":5555 ubuntu@localhost";
        String command8081 = "ssh -f -N -R 8081:" + ipAddress + ":8081 ubuntu@localhost";
        try
        {
            ResourceHost resourceHostByContainerId = localPeer.getResourceHostByContainerId ( containerHost.getId () );
            resourceHostByContainerId.execute ( new RequestBuilder ( command1443 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command5555 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command8081 ) );
        }
        catch ( HostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger ( ClusterConfiguration.class.getName () ).log ( Level.SEVERE, null, ex );
        }

    }


    private void makeCleanUpPreviousInstallation ( EnvironmentContainerHost containerHost )
    {
        this.commandExecute ( containerHost, "sudo rm /root/.ssh/*" );
        this.commandExecute ( containerHost, "sudo touch /root/.ssh/known_hosts" );
        this.commandExecute ( containerHost, "sudo rm /root/.appscale/*" );
    }


    private void commandExecute ( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            // downloading takes time...
            CommandResult responseFrom = containerHost.execute ( new RequestBuilder ( command ).withTimeout ( 8000 ) );
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
        LOG.info ( "AppScalefile file created." );
        this.commandExecute ( containerHost, "echo ips_layout: >> /root/AppScalefile" );
        LOG.info ( "ips_layout inserted" );
        String cmdmaster = "echo '  master : " + ipaddr + "' >> /root/AppScalefile";
        this.commandExecute ( containerHost, cmdmaster );
        LOG.info ( "master ip address inserted" );
        this.commandExecute ( containerHost, "echo '  appengine : " + ipaddr + "' >> /root/AppScalefile" );
        LOG.info ( "appengine ip address inserted" );
        if ( config.getZookeeperName () != null )
        {
            try
            {
                EnvironmentContainerHost zooContainerHost = environment.getContainerHostByHostname (
                        config.getZookeeperName () );
                String zooip = getIPAddress ( zooContainerHost );
                this.commandExecute ( containerHost,
                                      "echo '  zookeeper : " + zooip + "' >> /root/AppScalefile" );
                LOG.info ( "zookeeper ip address inserted" );

            }
            catch ( ContainerHostNotFoundException ex )
            {

            }
        }
        else
        {
            this.commandExecute ( containerHost, "echo '  zookeeper : " + ipaddr + "' >> /root/AppScalefile" );
            LOG.info ( "zookeeper ip address inserted" );
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
                LOG.info ( "cassandra ip address inserted" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "Environment can not be found..." + ex );
            }
        }
        else
        {
            this.commandExecute ( containerHost, "echo '  database : " + ipaddr + "' >> /root/AppScalefile" );
            LOG.info ( "cassandra ip address inserted" );
        }

        this.commandExecute ( containerHost, "cp /root/AppScalefile /" );

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

