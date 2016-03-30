package io.subutai.plugin.storm.impl;


import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.impl.alert.StormAlertListener;
import io.subutai.plugin.zookeeper.api.Zookeeper;


public abstract class StormBase implements Storm
{

    private static final Logger LOG = LoggerFactory.getLogger( StormImpl.class.getName() );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    protected Tracker tracker;
    protected Zookeeper zookeeperManager;
    protected EnvironmentManager environmentManager;
    protected PluginDAO pluginDAO;
    protected ExecutorService executor;
    protected PeerManager peerManager;
    protected Monitor monitor;
    protected StormAlertListener stormAlertListener;


    public void setExecutor( ExecutorService executor )
    {
        this.executor = executor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


   /* public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }*/


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void setMonitor( final Monitor monitor )
    {
        this.monitor = monitor;
    }


    /*public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( StormAlertListener.STORM_ALERT_LISTENER, environment, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( StormAlertListener.STORM_ALERT_LISTENER, environment );
    }*/


    public StormAlertListener getStormAlertListener()
    {
        return stormAlertListener;
    }


    public void setStormAlertListener( final StormAlertListener stormAlertListener )
    {
        this.stormAlertListener = stormAlertListener;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Zookeeper getZookeeperManager()
    {
        return zookeeperManager;
    }


    public void setZookeeperManager( Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public void setPluginDAO( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public Logger getLogger()
    {
        return LOG;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }
}
