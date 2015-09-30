package io.subutai.plugin.spark.impl;


import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SparkBase
{

    protected static final Logger LOG = LoggerFactory.getLogger( SparkBase.class.getName() );
    public PluginDAO pluginDAO;
    private Monitor monitor;

    Tracker tracker;
    EnvironmentManager environmentManager;
    Hadoop hadoopManager;

    ExecutorService executor;
    Commands commands;


    public void init()
    {
        this.commands = new Commands();
        executor = SubutaiExecutors.newCachedThreadPool();
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


    protected SparkBase( final Tracker tracker, final EnvironmentManager environmentManager, final Hadoop hadoopManager,
                         final Monitor monitor,final PluginDAO pluginDAO)
    {
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }
}
