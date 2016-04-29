/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.time.DateUtils;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.host.HostId;
import io.subutai.common.peer.AlertHandlerPriority;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.PeerException;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.exception.EnvironmentManagerException;
import io.subutai.core.identity.api.IdentityManager;
import io.subutai.core.identity.api.model.UserToken;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.handler.AppscaleAlertHandler;


/**
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final TrackerOperation po;
    private final AppScaleImpl appscaleManager;
    private final IdentityManager identityManager;
    private EnvironmentContainerHost containerHost;

    String token = "";


    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterConfiguration ( final TrackerOperation operation, final AppScaleImpl appScaleImpl,
                                  IdentityManager identityManager )
    {
        this.po = operation;
        this.appscaleManager = appScaleImpl;
        this.identityManager = identityManager;
    }


    /**
     * @throws ClusterConfigurationException configure cluster with appscale pre - requirements
     *
     * tutorial: https://confluence.subutai.io/display/APPSTALK/How+to+configure+container+for+the+needs+of+AppScale
     */
    @Override
    public void configureCluster ( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        AppScaleConfig config = ( AppScaleConfig ) configBase;

        if ( config.getPermanentToken () == null )
        {
            Date permanentDate = DateUtils.addYears ( new Date ( System.currentTimeMillis () ), 10 );
            final UserToken t = identityManager
                    .createUserToken ( identityManager.getActiveUser (), null, null, null, 2, permanentDate );
            token = t.getFullToken ();
            config.setPermanentToken ( token );
        }
        else
        {
            token = config.getPermanentToken ();
        }

        if ( "static".equals ( config.getScaleOption () ) ) // static is SS way of scaling
        {
            this.subutaiScaling ( configBase, environment ); // subutai scaling.
        }
        else
        {
            this.ASScaling ( configBase, environment );
        }
    }


    private void ASScaling ( ConfigBase configBase, Environment environment )
    {
        AppScaleConfig config = ( AppScaleConfig ) configBase;
        String userDomain = config.getUserDomain ();

        LOG.debug ( config.getUserDomain () + "\n" + config.getClusterName () + "\n" );

        if ( userDomain == null )
        {
            po.addLogFailed ( "User Domain Must be Set!" );
        }
        Set<EnvironmentContainerHost> cn = environment.getContainerHosts ();
        try
        {
            containerHost = environment.getContainerHostByHostname ( config.getClusterName () );

            LOG.info ( "Container Host Found: " + containerHost.getContainerId () + "\n" + "\n" + containerHost
                    .getHostname () + "\n" );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "configureCluster " + ex );
        }

        // save token to a file
        this.commandExecute ( containerHost, "echo '" + token + "' > /token" );
        this.commandExecute ( containerHost, Commands.getCreateLogDir () );
        this.appscaleInitCloud ( containerHost, environment, config );
        this.runAfterInitCommands ( containerHost, config );
        this.commandExecute ( containerHost, "sudo /root/appscale-tools/bin/appscale up" );
        config.setEnvironmentId ( environment.getId () );
        boolean saveInfo = appscaleManager.getPluginDAO ()
                .saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                            configBase );

        try
        {
            appscaleManager.getEnvironmentManager ()
                    .startMonitoring ( AppscaleAlertHandler.HANDLER_ID, AlertHandlerPriority.NORMAL,
                                       environment.getId () );
            po.addLog ( "Alert handler added successfully." );
        }
        catch ( EnvironmentManagerException e )
        {
            LOG.error ( e.getMessage (), e );
            po.addLogFailed ( "Could not add alert handler to monitor this environment." );
        }
        LOG.info ( "SAVE INFO: " + saveInfo );
        LOG.info ( "Appscale saved to database" );
        po.addLogDone ( "DONE" );
    }


    private void subutaiScaling ( ConfigBase configBase, Environment environment )
    {
        AppScaleConfig config = ( AppScaleConfig ) configBase;
        String userDomain = config.getUserDomain ();

        if ( userDomain == null )
        {
            po.addLogFailed ( "User Domain Must be Set!" );
        }

        Set<EnvironmentContainerHost> cn = environment.getContainerHosts ();
        int numberOfContainers = cn.size ();

        try
        {
            // this will be our controller container.
            containerHost = environment.getContainerHostByHostname ( config.getClusterName () );
            if ( containerHost.getContainerSize () != ContainerSize.HUGE )
            {
                LOG.error ( "Please select your container size as HUGE. Aborting" );
                po.addLogFailed ( "Please select your containers HUGE size. Aborting." );
            }
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "configureCluster " + ex );
            po.addLogFailed ( "Container Host Found: " + containerHost.getContainerId () + "\n" + "\n" + containerHost
                    .getHostname () + "\n" );
        }

        this.commandExecute ( containerHost, "echo '" + token + "' > /token" );
        this.commandExecute ( containerHost, Commands.getCreateLogDir () );
        LOG.info ( "installing appscale can take several minutes." );


        // this part will be removed
        this.commandExecute ( containerHost, "apt-get update" );
        this.commandExecute ( containerHost, "apt-get install expect -y" );

        // AppScalefile configuration
        this.appscaleInitCluster ( containerHost, environment, config ); // writes AppScalefile
        // this.appscaleInitIPS ( containerHost, environment, config );
        // end of AppScalefile configuration
        LOG.info ( "START AFTER INIT" );
        this.runAfterInitCommands ( containerHost, config );
        this.addKeyPairSH ( containerHost );
        //        this.runInstances ( containerHost );
        //        this.addKeyPairSHToExistance ( containerHost );
        //        this.commandExecute ( containerHost, "sudo /root/addKey.sh " + numberOfContainers );
        //        this.commandExecute ( containerHost, "sudo /root/runIns.sh " + 1 );
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
            LOG.error ( "RUN SHELL ERROR" + ex );
        }
        LOG.info ( "Run shell completed..." );
        this.createUpShell ( containerHost ); // later we can add button for start/stop to UI
        LOG.info ( "RH command started" );
        this.runInRH ( containerHost, config.getClusterName (), config );
        LOG.info ( "RH command ended" );
        config.setEnvironmentId ( environment.getId () );
        // add alert handler for scaleUp();
        try
        {
            appscaleManager.getEnvironmentManager ()
                    .startMonitoring ( AppscaleAlertHandler.HANDLER_ID, AlertHandlerPriority.NORMAL,
                                       environment.getId () );
            po.addLog ( "Alert handler added successfully." );
        }
        catch ( EnvironmentManagerException e )
        {
            LOG.error ( e.getMessage (), e );
            po.addLogFailed ( "Could not add alert handler to monitor this environment." );
        }
        this.commandExecute ( containerHost, "echo '127.0.1.1 appscale-image0' >> /etc/hosts" );
        this.commandExecute ( containerHost, Commands.backUpSSH () );
        this.commandExecute ( containerHost, Commands.backUpAppscale () );
        boolean saveInfo = appscaleManager.getPluginDAO ()
                .saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                            configBase );
        LOG.info ( "Appscale saved to database" );
        po.addLogDone ( "DONE" );
    }


    /**
     * @return this method only for the scale up appscale should be created already
     */
    public Boolean scaleUP ( ConfigBase conf, Environment env )
    {
        LOG.info ( "//// scaling started..." );

        Boolean scaled = false;
        AppScaleConfig localConfig = ( AppScaleConfig ) conf;

        for ( String a : localConfig.getAppenList () )
        {
            LOG.info ( "Appengine: " + a );
        }
        String appip = null;
        EnvironmentContainerHost containerHost = null;
        try
        {
            containerHost = env.getContainerHostByHostname ( localConfig.getClusterName () );
            appip = env.getContainerHostByHostname ( localConfig.getAppengine () )
                    .getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
            LOG.info ( "container host found..." );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( ex.toString () );
        }
        this.commandExecute ( containerHost, Commands.revertBackUpSSH () );
        this.commandExecute ( containerHost, Commands.revertBackupAppscale () );
        this.commandExecute ( containerHost, "rm -rf /root/sshBACK" );
        this.commandExecute ( containerHost, "rm -rf /root/appBACK" );
        LOG.info ( "appscale stopping" );
        this.commandExecute ( containerHost, Commands.getAppScaleStopCommand () ); // stop it
        this.makeCleanUpPreviousInstallation ( containerHost ); // this is just cleaning ssh etc..
        LOG.info ( "init cluster" );
        // this.appscaleInitIPS ( containerHost, env, localConfig ); // creates AppScalefile
        this.appscaleInitCluster ( containerHost, env, localConfig );
        //        String ipString = this.getIPAddress ( containerHost );
        //        String findthis = "  appengine:";
        //        String addthis = findthis + "\n" + "  - " + appip;
        //        String addcontainer = "sed -i 's/" + findthis + "/" + addthis + "/g' /AppScalefile";
        //        this.commandExecute ( containerHost, addcontainer );
        this.commandExecute ( containerHost, "cat /AppScalefile" );
        Set<EnvironmentContainerHost> cn = env.getContainerHosts ();
        int numberOfContainers = cn.size ();
        try
        {
            LOG.info ( "let's give some time to container to wake up" );
            TimeUnit.SECONDS.sleep ( 30 );
        }
        catch ( InterruptedException ex )
        {
            LOG.error ( ex.toString () );
        }
        //        this.commandExecute ( containerHost, "sudo /root/addKeyExistance.sh " + 1 );
        //        this.commandExecute ( containerHost, addInstances () );
        String runShell = Commands.getRunShell ();
        runShell = runShell + " " + numberOfContainers;

        try
        {
            containerHost.execute ( new RequestBuilder ( runShell ).withTimeout ( 10000 ) ); // will take time
            scaled = true;
            LOG.info ( "appscale restarted" );
        }
        catch ( CommandException ex )
        {
            LOG.error ( "RUN SHELL ERROR" + ex );
        }
        appscaleManager.getPluginDAO ().saveInfo ( AppScaleConfig.PRODUCT_KEY, conf.getClusterName (), conf );

        this.commandExecute ( containerHost, Commands.backUpAppscale () );
        this.commandExecute ( containerHost, Commands.backUpSSH () );
        return scaled;
    }


    private void runInRH ( EnvironmentContainerHost containerHost, String clusterName, AppScaleConfig config )
    {
        LOG.info ( "RUN IN RH" );
        PeerManager peerManager = appscaleManager.getPeerManager ();
        LOG.info ( "PeerManager: " + peerManager );
        LocalPeer localPeer = peerManager.getLocalPeer ();
        LOG.info ( "LocalPeer: " + localPeer );
        String ipAddress
                = containerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp (); // only for master

        try
        {
            ResourceHost resourceHostByContainerId = null;

            try
            {
                resourceHostByContainerId
                        = localPeer.getResourceHostByContainerId ( containerHost.getContainerId ().toString () );
            }
            catch ( HostNotFoundException e )
            {
                // if this happens then we do not have localhost. lets look for remote...
                LOG.error ( e.toString () );
                HostId hid = null;
                try
                {
                    hid = peerManager.getPeer ( peerManager.getRemotePeerIdByIp ( ipAddress ) )
                            .getResourceHostIdByContainerId ( containerHost.getContainerId () );
                    resourceHostByContainerId = ( ResourceHost ) hid;
                }
                catch ( PeerException ex )
                {
                    LOG.error ( ex.toString () );
                    po.addLogFailed ( "NO HOST FOUND!!!" );
                }
            }
            finally
            {
                if ( resourceHostByContainerId == null )
                {
                    LOG.error ( "still null" );
                    po.addLogFailed ( "resourceHostByContainerId can not be found " );
                }
            }

            LOG.info ( "resouceHostID: " + resourceHostByContainerId );
            LOG.info ( "HERE IS THE RESOURCE HOST: " + resourceHostByContainerId.getHostname () );

            CommandResult resultStr = resourceHostByContainerId
                    .execute ( new RequestBuilder ( "grep vlan /mnt/lib/lxc/" + clusterName + "/config" ) );
            String stdOut = resultStr.getStdOut ();
            String vlanString = stdOut.substring ( 11, 14 );
            resourceHostByContainerId.execute ( new RequestBuilder ( "subutai proxy del " + vlanString + " -d" ) );
            resourceHostByContainerId.execute ( new RequestBuilder (
                    "subutai proxy add " + vlanString + " -d \"*." + config.getUserDomain () + "\" -f /mnt/lib/lxc/"
                    + clusterName + "/rootfs/etc/nginx/ssl.pem" ) );
            resourceHostByContainerId
                    .execute ( new RequestBuilder ( "subutai proxy add " + vlanString + " -h " + ipAddress ) );
        }
        catch ( CommandException ex )
        {
            LOG.error ( ex.toString () );
            po.addLogFailed ( ex.toString () );
        }
        LOG.info ( "END OF RUN IN RH" );
    }


    // necessary for SS way of scaling.
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


    private void appscaleInitCloud ( EnvironmentContainerHost containerHost, Environment environment,
                                     AppScaleConfig config )
    {
        String ipaddr = containerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
        String ssConfig = "infrastructure: ss\n";
        ssConfig += "PARAM_IMAGE_ID: appscale\n";
        ssConfig += "PARAM_SUBUTAI_ENDPOINT: 1443." + config.getUserDomain () + "/rest/appscale/\n";
        ssConfig += "min: 1\n";
        ssConfig += "max: 1\n";
        ssConfig += "machine: appscale\n";
        this.commandExecute ( containerHost, "echo " + ssConfig + " > /AppScalefile" );

    }


    /**
     * @param config we will use bash commands to create AppScale file
     */
    private void appscaleInitCluster ( EnvironmentContainerHost containerHost, Environment environment,
                                       AppScaleConfig config )
    {
        String ipaddr = containerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
        String toPushinConfig = "ips_layout:\n";
        toPushinConfig += "  master : " + ipaddr + "\n";
        toPushinConfig += "  appengine:\n";

        List<String> appenList = config.getAppenList ();
        for ( String a : appenList )
        {

            try
            {
                EnvironmentContainerHost appContainerHost = environment.getContainerHostByHostname ( a );
                String aip = appContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                toPushinConfig += "  - " + aip + "\n";
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "AppScalefile: appengine: " + ex );
            }
        }
        toPushinConfig += "  zookeeper:\n";
        List<String> zooList = config.getZooList ();
        for ( String z : zooList )
        {
            try
            {
                EnvironmentContainerHost zooContainerHost = environment.getContainerHostByHostname ( z );
                String zip = zooContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                toPushinConfig += "  - " + zip + "\n";
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "AppScalefile: zookeeper: " + ex );
            }
        }
        toPushinConfig += "  database:\n";
        List<String> cassList = config.getCassList ();
        for ( String cis : cassList )
        {
            try
            {
                EnvironmentContainerHost cassContainerHost = environment.getContainerHostByHostname ( cis );
                String cip = cassContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                toPushinConfig += "  - " + cip + "\n";
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "AppScalefile : cassandra: " + ex );
            }
        }
        toPushinConfig += "login: " + config.getUserDomain () + "\n";
        LOG.info ( toPushinConfig );
        this.commandExecute ( containerHost, "echo -e '" + toPushinConfig + "' > /AppScalefile" );
        LOG.info ( "AppScalefile file created." );
    }


    /////////////////////////////////////////////////////
    private void appscaleInitIPS ( EnvironmentContainerHost containerHost, Environment environment,
                                   AppScaleConfig config )
    {
        String ipaddr = containerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
        this.commandExecute ( containerHost, "rm -f /root/ips.yaml && touch /root/ips.yaml" );
        LOG.info ( "ips.yaml file created." );
        String cmdmaster = "echo 'master : " + ipaddr + "' >> /root/ips.yaml";
        this.commandExecute ( containerHost, cmdmaster );
        LOG.info ( "master ip address inserted" );
        this.commandExecute ( containerHost, "echo 'appengine:' >> /root/ips.yaml" );
        List<String> appenList = config.getAppenList ();
        for ( String a : appenList )
        {

            try
            {
                EnvironmentContainerHost appContainerHost = environment.getContainerHostByHostname ( a );
                String aip = appContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                this.commandExecute ( containerHost, "echo '- " + aip + "' >> /root/ips.yaml" );
                LOG.info ( "appengine ip " + aip + " inserted" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "ips.yaml: appengine: " + ex );
            }
        }
        this.commandExecute ( containerHost, "echo 'zookeeper:' >> /root/ips.yaml" );
        List<String> zooList = config.getZooList ();
        for ( String z : zooList )
        {
            try
            {
                EnvironmentContainerHost zooContainerHost = environment.getContainerHostByHostname ( z );
                String zip = zooContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                this.commandExecute ( containerHost, "echo '- " + zip + "' >> /root/ips.yaml" );
                LOG.info ( "zookeeper ip " + zip + " inserted" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "ips.yaml: zookeeper: " + ex );
            }
        }
        this.commandExecute ( containerHost, "echo 'database:' >> /root/ips.yaml" );
        List<String> cassList = config.getCassList ();
        for ( String cis : cassList )
        {
            try
            {
                EnvironmentContainerHost cassContainerHost = environment.getContainerHostByHostname ( cis );
                String cip = cassContainerHost.getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                this.commandExecute ( containerHost, "echo '- " + cip + "' >> /root/ips.yaml" );
                LOG.info ( "cassandra ip " + cip + " inserted" );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( "ips.yaml : cassandra: " + ex );
            }
        }
        this.commandExecute ( containerHost, "echo login: " + ipaddr + " >> /root/ips.yaml" );
        this.commandExecute ( containerHost, "cp /root/ips.yaml /" );

        if ( config.getAppengine () != null )
        {
            this.commandExecute ( containerHost, "rm -f /root/new.yaml && touch /root/new.yaml" );
            try
            {
                String newip = environment.getContainerHostByHostname ( config.getAppengine () )
                        .getInterfaceByName ( Common.DEFAULT_CONTAINER_INTERFACE ).getIp ();
                this.commandExecute ( containerHost, "echo 'appengine:' >> /root/new.yaml" );
                this.commandExecute ( containerHost, "echo '- " + newip + "' >> /root/new.yaml" );
                this.commandExecute ( containerHost, "cp /root/new.yaml /" );
                config.setAppengine ( null );
            }
            catch ( ContainerHostNotFoundException ex )
            {
                LOG.error ( ex.toString () );
            }
        }
    }

    ///////////////////////////////////////////////////

    private void runAfterInitCommands ( EnvironmentContainerHost containerHost, AppScaleConfig config )
    {
        this.commandExecute ( containerHost,
                              "sed -i 's/{0}:{1}/{1}.{0}/g' /root/appscale/AppDashboard/lib/app_dashboard_data.py" );
        this.commandExecute ( containerHost, "echo -e '127.0.0.1 " + config.getUserDomain () + "' >> /etc/hosts" );
        this.commandExecute ( containerHost,
                              "sed -i 's/127.0.0.1 localhost.localdomain localhost/127.0.0.1 localhost.localdomain localhost "
                              + config.getUserDomain () + "/g' " + "/root/appscale/AppController/djinn.rb" );
        this.commandExecute ( containerHost,
                              "sed -i 's/127.0.0.1 localhost.localdomain localhost/127.0.0.1 localhost.localdomain localhost "
                              + config.getUserDomain () + "/g' " + "/etc/hosts" );

        this.commandExecute ( containerHost, "cat /etc/nginx/mykey.pem /etc/nginx/mycert.pem > /etc/nginx/ssl.pem" );

        // modify navigation.html
        // make them point to correct url in AS console
        String changeMonitURL = "sed -i 's/{{ monit_url }}/http:\\/\\/2812." + config.getUserDomain ()
                + "/g' /root/appscale/AppDashboard/templates/shared/navigation.html";
        this.commandExecute ( containerHost, changeMonitURL );
        String changeFlowerURL = "sed -i 's/{{ flower_url }}/http:\\/\\/5555." + config.getUserDomain ()
                + "/g' /root/appscale/AppDashboard/templates/shared/navigation.html";
        this.commandExecute ( containerHost, changeFlowerURL );
        String modUrl = "resturl?";
        String modUrlChange
                = "1443." + config.getUserDomain () + "\\/rest\\/appscale\\/growenvironment?containerName=" + config
                .getClusterName () + "&";
        this.commandExecute ( containerHost, "sed -i 's/" + modUrl + "/" + modUrlChange
                              + "/g' /root/appscale/AppDashboard/templates/shared/navigation.html" );
        // modify ss_agent.py
        // for now we can skip this since timur baike will provide ss_agent.py
//        String modstr = "thispathtochange = \"\\/rest\\/appscale\\/growenvironment?clusterName=\\\"";
//        String modstrchange
//                = "thispathtochange = \"\\/rest\\/appscale\\/growenvironment?clusterName=\"" + config.getClusterName ();
//        this.commandExecute ( containerHost, "sed -i 's/" + modstr + "/" + modstrchange
//                              + "/g' /root/appscale/InfrastructureManager/agents/ss_agent.py" );
//        String tokenUrl = "subutai:8443";
//        String tokenUrlChange = "1443." + config.getUserDomain ();
//        this.commandExecute ( containerHost, "sed -i 's/" + tokenUrl + "/" + tokenUrlChange
//                              + "/g' /root/appscale/InfrastructureManager/agents/ss_agent.py" );

        // modify nginx
        String nginx
                = "echo 'server {\n"
                + "        listen        80;\n"
                + "        server_name   ~^(?<port>.+)\\." + config.getUserDomain () + "$;\n" + "\n"
                + "    set $appbackend \"127.0.0.1:${port}\";\n" + "\n"
                + "    # proxy to AppScale over http\n"
                + "    if ($port = 1443) {\n"
                + "        set $appbackend \"appscaledashboard\";\n" + "    }\n" + "\n"
                + "    location / {\n"
                + "        proxy_pass http://$appbackend;\n"
                + "        proxy_set_header   X-Real-IP $remote_addr;\n"
                + "        proxy_set_header   Host $http_host;\n"
                + "        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;\n" + "\n" + "    }\n"
                + "}' > /etc/nginx/sites-enabled/default";

        this.commandExecute ( containerHost, nginx );
    }


    private void createRunSH ( EnvironmentContainerHost containerHost )
    {

        try
        {
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
        String runsh = "#!/usr/bin/expect -f\n" + "set timeout -1\n" + "set num $argv\n"
                + "spawn /root/appscale-tools/bin/appscale up\n" + "\n"
                + "#for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                + "#    expect \"Are you sure you want to continue connecting (yes/no)?\"\n"
                + "#    send -- \"yes\\n\"\n"
                + "#    expect \" password:\"\n"
                + "#    send -- \"a\\n\"\n"
                + "#}\n"
                + "\n"
                + "expect \"Enter your desired admin e-mail address:\"\n" + "send -- \"a@a.com\\n\"\n"
                + "expect \"Enter new password:\"\n" + "send -- \"aaaaaa\\n\"\n" + "expect \"Confirm password:\"\n"
                + "send -- \"aaaaaa\\n\"\n" + "\n" + "expect EOD";
        return runsh;
    }


    private void addKeyPairSH ( EnvironmentContainerHost containerHost )
    {
        try
        {
            String add = "#!/usr/bin/expect -f\n" + "set timeout -1\n" + "set num argv\n"
                    + "spawn /root/appscale-tools/bin/appscale-add-keypair --ips ips.yaml --keyname appscale\n"
                    + "for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                    + "    expect \"Are you sure you want to continue connecting (yes/no)?\"\n"
                    + "    send -- \"yes\\n\"\n" + "    expect \" password:\"\n" + "    send -- \"a\\n\"\n" + "}\n"
                    + "expect EOD";
            containerHost.execute ( new RequestBuilder ( "mkdir .ssh" ) );
            containerHost.execute ( new RequestBuilder ( "echo '" + add + "' > /root/addKey.sh" ) );
            containerHost.execute ( new RequestBuilder ( "chmod +x /root/addKey.sh" ) );
        }
        catch ( CommandException ex )
        {
            LOG.error ( ex.toString () );
        }
    }


    private void addKeyPairSHToExistance ( EnvironmentContainerHost containerHost )
    {
        try
        {
            String add = "#!/usr/bin/expect -f\n" + "set timeout -1\n" + "set num argv\n"
                    + "spawn /root/appscale-tools/bin/appscale-add-keypair --ips new.yaml --add_to_existing --keyname"
                    + " appscale\\n" + "for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                    + "    expect \"Are you sure you want to continue connecting (yes/no)?\"\n"
                    + "    send -- \"yes\\n\"\n" + "    expect \" password:\"\n" + "    send -- \"a\\n\"\n" + "}\n"
                    + "expect EOD";

            containerHost.execute ( new RequestBuilder ( "rm /root/addKeyExistance.sh " ) );
            containerHost.execute ( new RequestBuilder ( "touch /root/addKeyExistance.sh" ) );
            containerHost.execute ( new RequestBuilder ( "echo '" + add + "' > /root/addKeyExistance.sh" ) );
            containerHost.execute ( new RequestBuilder ( "chmod +x /root/addKeyExistance.sh" ) );
        }
        catch ( CommandException ex )
        {
            LOG.error ( ex.toString () );
        }
    }


    private void runInstances ( EnvironmentContainerHost containerHost )
    {
        try
        {
            String runins = "#!/usr/bin/expect -f\n" + "set timeout -1\n" + "set num $argv\n"
                    + "spawn /root/appscale-tools/bin/appscale-run-instances --ips ips.yaml -v --keyname appscale\n"
                    + "for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                    + "expect \"Enter your desired admin e-mail address:\"\n" + "send -- \"a@a.com\\n\"\n"
                    + "expect \"Enter new password:\"\n" + "send -- \"aaaaaa\\n\"\n" + "expect \"Confirm password:\"\n"
                    + "send -- \"aaaaaa\\n\"" + "}\n" + "expect EOD";
            containerHost.execute ( new RequestBuilder ( "rm /root/runIns.sh " ) );
            containerHost.execute ( new RequestBuilder ( "touch /root/runIns.sh" ) );
            containerHost.execute ( new RequestBuilder ( "echo '" + runins + "' > /root/runIns.sh" ) );
            containerHost.execute ( new RequestBuilder ( "chmod +x /root/runIns.sh" ) );
        }
        catch ( CommandException ex )
        {
            LOG.error ( ex.toString () );
        }
    }


    private String addInstances ()
    {
        String addIns = "sudo /root/appscale-tools/bin/appscale-add-instances --ips new.yaml --keyname appscale";
        return addIns;
    }


    private void createUpShell ( EnvironmentContainerHost containerHost )
    {

        String a = null;
        a = "#!/usr/bin/expect -f\n" + "set timeout -1\n" + "set num $argv\n"
                + "spawn /root/appscale-tools/bin/appscale up\n" + "\n"
                + "for {set i 1} {\"$i\" <= \"$num\"} {incr i} {\n"
                + "expect \"Enter your desired admin e-mail address:\"\n" + "send -- \"a@a.com\\n\"\n"
                + "expect \"Enter new password:\"\n" + "send -- \"aaaaaa\\n\"\n" + "expect \"Confirm password:\"\n"
                + "send -- \"aaaaaa\\n\"\n" + "\n" + "}\n" + "\n" + "expect EOD";
        Environment env;
        try
        {

            containerHost.execute ( new RequestBuilder ( "touch /root/up.sh" ) );
            containerHost.execute ( new RequestBuilder ( "echo '" + a + "' > /root/up.sh" ) );
            containerHost.execute ( new RequestBuilder ( "chmod +x /root/up.sh" ) );
        }
        catch ( CommandException ex )
        {
            LOG.error ( "Error on create up shell: " + ex );
        }
    }
}

