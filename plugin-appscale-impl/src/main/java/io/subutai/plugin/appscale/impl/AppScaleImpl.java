/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;
import io.subutai.plugin.appscale.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.PluginDAO;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class AppScaleImpl implements AppScaleInterface, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger ( AppScaleImpl.class.getName () );
    private ExecutorService executor;
    private Monitor monitor;
    private PluginDAO pluginDAO;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private NetworkManager networkManager;


    public AppScaleImpl ( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public void init ()
    {
        executor = SubutaiExecutors.newCachedThreadPool ();
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
        Preconditions.checkNotNull ( appScaleConfig, "Configuration is null" );
        Preconditions.checkArgument ( !Strings.isNullOrEmpty ( appScaleConfig.getClusterName () ),
                                      "Clustername is empty or null" );
        AbstractOperationHandler abstractOperationHandler = new ClusterOperationHandler ( this, appScaleConfig,
                                                                                          ClusterOperationType.INSTALL );
        executor.execute ( abstractOperationHandler );
        return abstractOperationHandler.getTrackerId ();
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
    public UUID uninstallCluster ( String string )
    {
        return uninstallCluster ( getConfig ( string ) );
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
    public void saveCongig ( AppScaleConfig ac )
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
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public List<AppScaleConfig> getClusters ()
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public AppScaleConfig getCluster ( String string )
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


    public void setExecutor ( ExecutorService executor )
    {
        this.executor = executor;
    }


    public ExecutorService getExecutor ()
    {
        return executor;
    }


    public void setMonitor ( Monitor monitor )
    {
        this.monitor = monitor;
    }


    public Monitor getMonitor ()
    {
        return monitor;
    }


    public void setPluginDAO ( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public PluginDAO getPluginDAO ()
    {
        return pluginDAO;
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


    public static Logger getLOG ()
    {
        return LOG;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }
    
    
}

