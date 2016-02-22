/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.Properties;
import java.util.Set;
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
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.appscale.api.AppScaleConfig;


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
        Set<EnvironmentContainerHost> cn = environment.getContainerHosts ();
        Properties p = System.getProperties ();
        p.setProperty ( "user.dir", "/root" );
        int numberOfContainers = cn.size ();

        try
        {
            containerHost = environment.getContainerHostByHostname ( config.getClusterName () );

            LOG.info (
                    "Container Host Found: " + containerHost.getContainerId () + "\n"
                    + "\n" + containerHost.getHostname () + "\n" );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "configureCluster " + ex );
            // po.addLogFailed( "container host is not found : " + ex );
        }


        // this.commandExecute ( containerHost, Commands.getRemoveSubutaiList () );
        this.commandExecute ( containerHost, Commands.getCreateLogDir () );

        LOG.info ( "installing appscale can take several minutes." );
        // AppScalefile configuration
        this.appscaleInitCluster ( containerHost, environment, config ); // writes AppScalefile
        // end of AppScalefile configuration
        LOG.info ( "cleaning up..." );
        this.makeCleanUpPreviousInstallation ( containerHost );
        LOG.info ( "clean up ended..." );
        LOG.info ( "Run shell starting..." );
        this.createRunSH ( containerHost ); // we only need this in master container...
        String runShell = Commands.getRunShell ();
        runShell = runShell + " " + numberOfContainers;
        LOG.info ( "RUN SHELL COMMAND: " + runShell );
        try
        {
            containerHost.execute ( new RequestBuilder ( runShell ).withTimeout ( 10000 ) );
        }
        catch ( CommandException ex )
        {
            java.util.logging.Logger.getLogger ( ClusterConfiguration.class.getName () ).log ( Level.SEVERE, null, ex );
        }
        LOG.info ( "Run shell completed..." );

        LOG.info ( "Login into your RH and run these commands: \n"
                + "ssh -f -N -R 1443:<Your RH IP>:1443 ubuntu@localhost\n"
                + "ssh -f -N -R 5555:<Your RH IP>:5555 ubuntu@localhost\n"
                + "ssh -f -N -R 8081:<Your RH IP>:8081 ubuntu@localhost\n" );

        config.setEnvironmentId ( environment.getId () );
        appscaleManager.getPluginDAO ().saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                                                   configBase );
        LOG.info ( "Appscale saved to database" );
        po.addLogDone ( "Appscale is saved to database" ); // will try to last po.addLogDone... nothing else..
        // this.runShhInResourceHost ( containerHost );

    }


    public void runShhInResourceHost ( EnvironmentContainerHost containerHost )
    {
        PeerManager peerManager = appscaleManager.getPeerManager ();
        LocalPeer localPeer = peerManager.getLocalPeer ();
        String ipAddress = this.getIPAddress ( containerHost ); // only for master
        String command1443 = "ssh -f -N -R 1443:" + ipAddress + ":1443 ubuntu@localhost";
        String command5555 = "ssh -f -N -R 5555:" + ipAddress + ":5555 ubuntu@localhost";
        String command8080 = "ssh -f -N -R 8081:" + ipAddress + ":8080 ubuntu@localhost";
        String command8081 = "ssh -f -N -R 8081:" + ipAddress + ":8081 ubuntu@localhost";
        try
        {
            ResourceHost resourceHostByContainerId = localPeer.getResourceHostByContainerId ( containerHost.getId () );
            LOG.info ( "HERE IS RESOURCE HOST: " + resourceHostByContainerId.getHostname () );
            resourceHostByContainerId.execute ( new RequestBuilder ( command1443 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command5555 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command8080 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command8081 ) );
        }
        catch ( HostNotFoundException | CommandException ex )
        {
            LOG.error ( ex.toString () );
        }

    }


    private void makeCleanUpPreviousInstallation ( EnvironmentContainerHost containerHost )
    {
        try
        {
            CommandResult cr;
            cr = containerHost.execute ( new RequestBuilder ( "ls /root/.ssh" ) );
            if ( !cr.toString ().equals ( "" ) )
            {
                this.commandExecute ( containerHost, "rm /root/.ssh/*" );
                this.commandExecute ( containerHost, "touch /root/.ssh/known_hosts" );
            }
            cr = containerHost.execute ( new RequestBuilder ( "ls /root/.appscale" ) );
            if ( !cr.toString ().equals ( "" ) )
            {
                this.commandExecute ( containerHost, "rm /root/.appscale/*" );
            }

        }
        catch ( CommandException ex )
        {
            LOG.error ( " clean process command exception: " + ex );
            // po.addLogFailed( "Clean up failed..." );
        }
    }


    private void commandExecute ( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            // downloading takes time...
            CommandResult responseFrom = containerHost.execute ( new RequestBuilder ( command ).withTimeout ( 10000 ) );

        }
        catch ( CommandException e )
        {
            LOG.error ( "Error while executing \"" + command + "\".\n" + e );
            // po.addLogFailed( command + " can not be executed properly..." );
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
                LOG.error ( ex.toString () );
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


    private void createRunSH ( EnvironmentContainerHost containerHost )
    {

        try
        {
            containerHost.execute ( new RequestBuilder ( "rm /root/run.sh " ) );
            containerHost.execute ( new RequestBuilder ( "touch /root/run.sh" ) );
            containerHost.execute ( new RequestBuilder ( "echo '" + returnRunSH () + "' > /root/run.sh" ) );
            containerHost.execute ( new RequestBuilder ( "chmod +x /root/run.sh" ) );
            LOG.info ( "RUN.SH CREATED..." );
        }
        catch ( CommandException ex )
        {
            LOG.error ( "createRunSSH error" + ex );
        }


    }


    private String returnRunSH ()
    {
        String sh = "#!/usr/bin/expect -f\n"
                + "set timeout -1\n"
                + "set num $argv\n"
                + "spawn /root/appscale-tools/bin/appscale up\n"
                + "\n"
                + "for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                + "    expect \"Are you sure you want to continue connecting (yes/no)?\"\n"
                + "    send -- \"yes\\n\"\n"
                + "    expect \" password:\"\n"
                + "    send -- \"a\\n\"\n"
                + "}\n"
                + "\n"
                + "expect \"Enter your desired admin e-mail address:\"\n"
                + "send -- \"a@a.com\\n\"\n"
                + "expect \"Enter new password:\"\n"
                + "send -- \"aaaaaa\\n\"\n"
                + "expect \"Confirm password:\"\n"
                + "send -- \"aaaaaa\\n\"\n"
                + "\n"
                + "expect EOD";

        return sh;

    }


}

