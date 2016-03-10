/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.api.UsergridInterface;
import io.subutai.plugin.usergrid.impl.handler.ClusterOperationHandler;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class UsergridIMPL implements UsergridInterface, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger ( UsergridIMPL.class.getName () );
    private ExecutorService executor;
    private final Monitor monitor;
    private final PluginDAO pluginDAO;
    private Tracker tracker;
    private NetworkManager networkManager;
    private EnvironmentManager environmentManager;
    private PeerManager peerManager;
    private Environment environment;
    private UsergridConfig userGridConfig;


    public UsergridIMPL ( Monitor monitor, PluginDAO pluginDAO )
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


    @Override
    public List<String> getClusterList ( Environment name )
    {
        List<String> c = new ArrayList ();
        Set<EnvironmentContainerHost> containerHosts = name.getContainerHosts ();
        containerHosts.stream ().forEach ( (e)
                ->
                {
                    c.add ( e.getHostname () );
        } );
        return c;
    }


    @Override
    public UUID installCluster ( UsergridConfig usergridConfig )
    {
        LOG.info ( "Install cluster Started..." );
        Preconditions.checkNotNull ( usergridConfig, "Configuration is null" );
        Preconditions.checkArgument (
                !Strings.isNullOrEmpty ( usergridConfig.getClusterName () ), "clusterName is empty or null" );
        AbstractOperationHandler abstractOperationHandler = new ClusterOperationHandler ( this, usergridConfig,
                                                                                          ClusterOperationType.INSTALL );
        executor.execute ( abstractOperationHandler );
//        boolean saveInfo = getPluginDAO ().saveInfo ( UsergridConfig.PRODUCT_KEY, usergridConfig.getClusterName (),
//                                                      usergridConfig );
//        if ( saveInfo )
//        {
//            LOG.info ( "Configuration saved to db" );
//        }
        return abstractOperationHandler.getTrackerId ();

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
    public void saveConfig ( UsergridConfig ac ) throws ClusterException
    {
        if ( !getPluginDAO ().saveInfo ( UsergridConfig.PRODUCT_KEY, ac.getClusterName (), ac ) )
        {
            throw new ClusterException ( "Could not save cluster info" );
        }
    }


    @Override
    public UsergridConfig getConfig ( String clusterName )
    {
        return this.userGridConfig;
    }


    @Override
    public UUID startCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID stopCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
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
    public UUID uninstallCluster ( String string )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public List<UsergridConfig> getClusters ()
    {
        return this.pluginDAO.getInfo ( UsergridConfig.PRODUCT_NAME, UsergridConfig.class );
    }


    @Override
    public UsergridConfig getCluster ( String string )
    {
        return pluginDAO.getInfo ( UsergridConfig.PACKAGE_NAME, string, UsergridConfig.class );
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


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
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


    public UsergridConfig getUsergridConfig ()
    {
        return userGridConfig;
    }


    public void setUsergridConfig ( UsergridConfig usergridConfig )
    {
        this.userGridConfig = usergridConfig;
    }


    public PluginDAO getPluginDAO ()
    {
        return pluginDAO;
    }


}

