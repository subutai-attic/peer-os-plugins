package org.safehaus.subutai.plugin.spark.impl;


import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SparkBase
{

    private static final Logger LOG = LoggerFactory.getLogger( SparkBase.class.getName() );
    public PluginDAO pluginDAO;
    public DataSource dataSource;
    private Monitor monitor;
    private QuotaManager quotaManager;

    Tracker tracker;
    EnvironmentManager environmentManager;
    Hadoop hadoopManager;

    ExecutorService executor;
    Commands commands;


    public void init()
    {
        try
        {
            this.pluginDAO = new PluginDAO( dataSource );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        this.commands = new Commands();

        executor = Executors.newCachedThreadPool();
    }


    public Commands getCommands()
    {
        return commands;
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    protected SparkBase( final DataSource dataSource, final Tracker tracker,
                         final EnvironmentManager environmentManager, final Hadoop hadoopManager, final Monitor monitor,
                         final QuotaManager quotaManager )
    {
        this.dataSource = dataSource;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.monitor = monitor;
        this.quotaManager = quotaManager;
    }
}
