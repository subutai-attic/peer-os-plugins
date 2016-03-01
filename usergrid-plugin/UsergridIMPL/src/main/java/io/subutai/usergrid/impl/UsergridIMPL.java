/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.usergrid.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.usergrid.api.UsergridConfig;
import io.subutai.usergrid.api.UsergridInterface;


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
    private EnvironmentManager environmentManager;
    private NetworkManager networkManager;
    private QuotaManager quotaManager;
    private PeerManager peerManager;
    private Environment environment;
    private UsergridConfig usergridConfig;


    public UsergridIMPL ( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    @Override
    public List<String> getClusterList ( Environment name )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID installCluster ( UsergridConfig usergridConfig )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void saveConfig ( UsergridConfig ac ) throws ClusterException
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UsergridConfig getConfig ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
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
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UsergridConfig getCluster ( String string )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
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


    public UsergridConfig getUsergridConfig ()
    {
        return usergridConfig;
    }


    public void setUsergridConfig ( UsergridConfig usergridConfig )
    {
        this.usergridConfig = usergridConfig;
    }

}

