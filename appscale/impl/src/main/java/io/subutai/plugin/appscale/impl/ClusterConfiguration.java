/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.network.ProxyLoadBalanceStrategy;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.protocol.ReverseProxyConfig;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.appscale.api.AppScaleConfig;

import static java.lang.String.format;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final LocalPeer localPeer;
    private final TrackerOperation po;
    private final AppScaleImpl appscaleManager;
    private EnvironmentContainerHost containerHost;


    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );


    public ClusterConfiguration( final TrackerOperation operation, final AppScaleImpl appScaleImpl,
                                 final PeerManager peerManager )
    {
        this.po = operation;
        this.appscaleManager = appScaleImpl;
        this.localPeer = peerManager.getLocalPeer();
    }


    @Override
    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        LOG.info( "in configureCluster" );
        AppScaleConfig config = ( AppScaleConfig ) configBase;
        this.installCluster( configBase, environment );
    }


    private void installCluster( ConfigBase configBase, Environment environment )
    {
        AppScaleConfig config = ( AppScaleConfig ) configBase;
        String domain = config.getDomain();

        if ( domain == null )
        {
            po.addLogFailed( "Missing domain for appscale" );
        }

        Set<EnvironmentContainerHost> cn = environment.getContainerHosts();
        int numberOfContainers = cn.size();


        try
        {
            // this will be our controller container.
            containerHost = environment.getContainerHostByHostname( config.getControllerNode() );
            if ( containerHost.getContainerSize() != ContainerSize.HUGE )
            {
                LOG.error( "Please make sure your containers' sizes are HUGE, disk quota is too small for your "
                        + "containers" );
                po.addLogFailed( "Please make sure your containers' sizes are HUGE, disk quota is too small for your "
                        + "containers" );
            }
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error( ex.getMessage() );
            po.addLogFailed( "Container Host Not Found" );
        }

        LOG.info( "Preparing AppScalefile" );
        // AppScalefile configuration
        this.appscaleInitCluster( containerHost, config );

        LOG.info( "Preparing domain and starting cluster" );
        // appscale up and domain management
        this.runAfterInitCommands( containerHost, config );
        LOG.info( "Configuring proxy on RH" );
        // configure proxy on rh
        this.configureRH( containerHost, config );

/*        try
        {
            appscaleManager.getEnvironmentManager()
                           .startMonitoring( AppscaleAlertHandler.HANDLER_ID, AlertHandlerPriority.NORMAL,
                                   environment.getId() );
            po.addLog( "Alert handler added successfully." );
        }
        catch ( EnvironmentManagerException e )
        {
            LOG.error( e.getMessage(), e );
            po.addLogFailed( "Could not add alert handler to monitor this environment." );
        }*/
        LOG.info( "Appscale installation done" );

        boolean saveInfo = appscaleManager.getPluginDAO()
                                          .saveInfo( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName(),
                                                  configBase );
        if ( saveInfo )
        {
            LOG.info( "Appscale saved to DB" );
            po.addLogDone( "DONE" );
        }
        else
        {
            LOG.error( "Could not save to DB" );
        }
    }


    private void configureRH( EnvironmentContainerHost containerHost, AppScaleConfig config )
    {
        try
        {
            ReverseProxyConfig proxyConfig =
                    new ReverseProxyConfig( config.getEnvironmentId(), containerHost.getId(), "*." + config.getDomain(),
                            "/mnt/lib/lxc/" + config.getControllerNode() + "/rootfs/etc/nginx/ssl.pem",
                            ProxyLoadBalanceStrategy.NONE );

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
            CommandResult result = containerHost.execute( new RequestBuilder( command ).withTimeout( 10000 ) );

            if ( result.getExitCode() != 0 )
            {
                throw new CommandException( format( "Error to execute command: %s. %s", command, result.getStdErr() ) );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( "Error while executing \"" + command + "\".\n" + e );
        }
    }


    private void appscaleInitCluster( EnvironmentContainerHost containerHost, AppScaleConfig config )
    {
        String ipaddr = containerHost.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp();
        String command = "sudo /var/lib/appscale/create-appscalefile.sh -master " + ipaddr + " -appengine";
        for ( int i = 0; i < config.getAppengineNodes().size(); ++i )
        {
            command += " " + config.getAppengineNodes().get( i );
        }
        command += " -database";
        for ( int i = 0; i < config.getCassandraNodes().size(); ++i )
        {
            command += " " + config.getCassandraNodes().get( i );
        }
        command += " -zookeeper ";
        for ( int i = 0; i < config.getZookeeperNodes().size(); ++i )
        {
            command += " " + config.getZookeeperNodes().get( i );
        }
        LOG.info( "Executing: " + command );
        this.commandExecute( containerHost, command );
    }


    private void runAfterInitCommands( EnvironmentContainerHost containerHost, AppScaleConfig config )
    {
        String cmd = format( "sudo /var/lib/appscale/setup.sh %s %s %s", config.getDomain(), config.getLogin(),
                config.getPassword() );

        LOG.info( "Executing: " + cmd );
        this.commandExecute( containerHost, cmd );
    }
}

