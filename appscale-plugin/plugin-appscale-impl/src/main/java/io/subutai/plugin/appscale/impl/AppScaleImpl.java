/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.HostNotFoundException;
import io.subutai.common.peer.LocalPeer;
import io.subutai.common.peer.ResourceHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.*;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;
import io.subutai.plugin.appscale.impl.handler.ClusterOperationHandler;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class AppScaleImpl implements AppScaleInterface, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger ( AppScaleImpl.class.getName () );
    private ExecutorService executor;
    private final Monitor monitor;
    private final PluginDAO pluginDAO;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private NetworkManager networkManager;
    private QuotaManager quotaManager;
    private PeerManager peerManager;
    private Environment environment;
    private AppScaleConfig appScaleConfig;


    public AppScaleImpl ( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public void init ()
    {
        executor = SubutaiExecutors.newCachedThreadPool ();
    }


    public void destroy ()
    {

    }


    /**
     *
     * @param appScaleConfig
     * @return
     *
     * setup -> install
     *
     */
    @Override
    public UUID installCluster ( AppScaleConfig appScaleConfig )
    {
        LOG.info ( "install cluster started" );

        Preconditions.checkNotNull ( appScaleConfig, "Configuration is null" );
        Preconditions.checkArgument (
                !Strings.isNullOrEmpty ( appScaleConfig.getClusterName () ), "Clustername is empty or null" );


        AbstractOperationHandler abstractOperationHandler = new ClusterOperationHandler ( this, appScaleConfig,
                                                                                          ClusterOperationType.INSTALL );
        LOG.info ( "install cluster " + abstractOperationHandler );
        executor.execute ( abstractOperationHandler );
        LOG.info ( "install executor " + " tracker id: " + abstractOperationHandler.getTrackerId () );
        getPluginDAO ()
                .saveInfo ( AppScaleConfig.PRODUCT_KEY, appScaleConfig.getClusterName (), appScaleConfig );
        return abstractOperationHandler.getTrackerId ();
    }


    @Override
    /**
     * returns true if container installed
     */
    public Boolean checkIfContainerInstalled ( AppScaleConfig appScaleConfig )
    {
        Boolean ret = true;

        Preconditions.checkNotNull ( appScaleConfig, "Configuration is null" );
        Preconditions.checkArgument (
                !Strings.isNullOrEmpty ( appScaleConfig.getClusterName () ), "Clustername is empty or null" );

        try
        {
            EnvironmentContainerHost containerHostByHostname = environment.getContainerHostByHostname (
                    appScaleConfig.getClusterName () );
            CommandResult commandResult = containerHostByHostname.execute ( new RequestBuilder ( Commands.getPsAUX () ) );
            if ( commandResult.getStdOut ().contains ( "No such file or directory" ) )
            {
                ret = false;
            }
        }
        catch ( ContainerHostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger ( AppScaleImpl.class.getName () ).log ( Level.SEVERE, null, ex );
        }
        return ret;
    }


    @Override
    public UUID uninstallCluster ( AppScaleConfig appScaleConfig )
    {
        Preconditions.checkNotNull ( appScaleConfig, "Configuration is null" );
        Preconditions.checkArgument ( !Strings.isNullOrEmpty ( appScaleConfig.getClusterName () ),
                                      "Clustername is empty" );
        AbstractOperationHandler abstractOperationHandler = new ClusterOperationHandler ( this, appScaleConfig,
                                                                                          ClusterOperationType.UNINSTALL );
        executor.execute ( abstractOperationHandler );
        return abstractOperationHandler.getTrackerId ();
    }


    @Override
    public void configureSsh ( AppScaleConfig appScaleConfig )
    {
        try
        {
            EnvironmentContainerHost containerHost = environment.getContainerHostByHostname (
                    appScaleConfig.getClusterName () );
            String ipAddress = this.getIPAddress ( containerHost );
            String command1443 = "ssh -f -N -R 1443:" + ipAddress + ":1443 ubuntu@localhost";
            String command5555 = "ssh -f -N -R 5555:" + ipAddress + ":5555 ubuntu@localhost";
            String command8081 = "ssh -f -N -R 8081:" + ipAddress + ":8081 ubuntu@localhost";

            LocalPeer localPeer = peerManager.getLocalPeer ();
            ResourceHost resourceHostByContainerId = localPeer.getResourceHostByContainerId ( containerHost.getId () );
            resourceHostByContainerId.execute ( new RequestBuilder ( command8081 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command1443 ) );
            resourceHostByContainerId.execute ( new RequestBuilder ( command5555 ) );

        }
        catch ( ContainerHostNotFoundException | HostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger ( AppScaleImpl.class.getName () ).log ( Level.SEVERE, null, ex );
        }
    }


    public AppScaleConfig getAppScaleConfig ()
    {
        return appScaleConfig;
    }


    public void setAppScaleConfig ( AppScaleConfig appScaleConfig )
    {
        this.appScaleConfig = appScaleConfig;
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


    @Override
    public UUID configureSSH ( AppScaleConfig appScaleConfig )
    {

        AbstractOperationHandler abstractOperationHandler = new ClusterOperationHandler ( this, appScaleConfig,
                                                                                          ClusterOperationType.CUSTOM );
        executor.execute ( abstractOperationHandler );
        return abstractOperationHandler.getTrackerId ();

    }


    @Override
    public List<String> getClusterList ( Environment name )
    {
        List<String> c = new ArrayList ();
        Set<EnvironmentContainerHost> containerHosts = name.getContainerHosts ();
        containerHosts.stream ().forEach ( (ech)
                ->
                {
                    c.add ( ech.toString () );
        } );
        return c;
    }


    @Override
    public UUID uninstallCluster ( String string )
    {
        return uninstallCluster ( getConfig ( string ) );
    }


    @Override
    public UUID startCluster ( String clusterName )
    {

        UUID uuid = null;
        try
        {
            EnvironmentContainerHost masterContainerHost = environment.getContainerHostByHostname ( clusterName );
            AbstractOperationHandler a = ( AbstractOperationHandler ) masterContainerHost.execute ( new RequestBuilder (
                    Commands.getAppScaleStartCommand () ) );
            uuid = a.getTrackerId ();
        }
        catch ( ContainerHostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger ( AppScaleImpl.class.getName () ).log ( Level.SEVERE, null, ex );
        }
        return uuid;

    }


    @Override
    public UUID stopCluster ( String clusterName )
    {
        UUID uuid = null;
        try
        {
            EnvironmentContainerHost masterContainerHost = environment.getContainerHostByHostname ( clusterName );
            AbstractOperationHandler a = ( AbstractOperationHandler ) masterContainerHost.execute ( new RequestBuilder (
                    Commands.getAppScaleStopCommand () ) );
            uuid = a.getTrackerId ();
        }
        catch ( ContainerHostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger ( AppScaleImpl.class.getName () ).log ( Level.SEVERE, null, ex );
        }
        return uuid;
    }


    @Override
    public UUID restartCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID statusCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID startService ( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID stopService ( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID statusService ( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID addNode ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID destroyNode ( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID removeCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy ( Environment e, TrackerOperation t, AppScaleConfig ac )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void saveConfig ( AppScaleConfig ac )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void deleteConfig ( AppScaleConfig ac )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public AppScaleConfig getConfig ( String clusterName )
    {
        return this.appScaleConfig;
    }


    @Override
    public List<AppScaleConfig> getClusters ()
    {
        return pluginDAO.getInfo ( AppScaleConfig.PRODUCT_KEY, AppScaleConfig.class );
    }


    @Override
    public AppScaleConfig getCluster ( String string )
    {
        return pluginDAO.getInfo ( AppScaleConfig.PRODUCT_KEY, string, AppScaleConfig.class );
    }


    @Override
    public UUID addNode ( String string, String string1 )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void onEnvironmentCreated ( Environment e )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void onEnvironmentGrown ( Environment e, Set<EnvironmentContainerHost> set )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void onContainerDestroyed ( Environment e, String string )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void onEnvironmentDestroyed ( String string )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    public ExecutorService getExecutor ()
    {
        return executor;
    }


    public void setExecutor ( ExecutorService executor )
    {
        this.executor = executor;
    }


    public Monitor getMonitor ()
    {
        return monitor;
    }


    public PluginDAO getPluginDAO ()
    {
        return pluginDAO;
    }


    public EnvironmentManager getEnvironmentManager ()
    {
        return environmentManager;
    }


    public void setEnvironmentManager ( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public NetworkManager getNetworkManager ()
    {
        return networkManager;
    }


    public void setNetworkManager ( NetworkManager networkManager )
    {
        this.networkManager = networkManager;
    }


    public QuotaManager getQuotaManager ()
    {
        return quotaManager;
    }


    public void setQuotaManager ( QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public PeerManager getPeerManager ()
    {
        return peerManager;
    }


    public void setPeerManager ( PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public Environment getEnvironment ()
    {
        return environment;
    }


    public void setEnvironment ( Environment environment )
    {
        this.environment = environment;
    }


    public static Logger getLOG ()
    {
        return LOG;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


}

