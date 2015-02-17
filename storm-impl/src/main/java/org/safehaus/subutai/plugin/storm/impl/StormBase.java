package org.safehaus.subutai.plugin.storm.impl;


import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.impl.alert.StormAlertListener;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.safehaus.subutai.plugin.common.PluginDAO;


public abstract class StormBase implements Storm
{

    private static final Logger LOG = LoggerFactory.getLogger( StormImpl.class.getName() );
    protected Tracker tracker;
    protected Zookeeper zookeeperManager;
    protected EnvironmentManager environmentManager;

    protected PluginDAO pluginDAO;
    protected ExecutorService executor;
    protected PeerManager peerManager;
    protected Monitor monitor;
    private StormAlertListener stormAlertListener;

    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    public void setExecutor(ExecutorService executor)
    {
        this.executor = executor;
    }

    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }

    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


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
        try
        {
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }

        executor = Executors.newCachedThreadPool();
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void setMonitor( final Monitor monitor )
    {
        this.monitor = monitor;
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
