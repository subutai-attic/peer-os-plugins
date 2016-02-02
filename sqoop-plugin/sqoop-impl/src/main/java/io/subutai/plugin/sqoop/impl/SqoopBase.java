package io.subutai.plugin.sqoop.impl;


import java.util.concurrent.ExecutorService;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.sqoop.api.Sqoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SqoopBase implements Sqoop
{

    static final Logger LOG = LoggerFactory.getLogger( SqoopImpl.class );

    Tracker tracker;
    Hadoop hadoopManager;
    EnvironmentManager environmentManager;

    PluginDAO pluginDAO;


    protected ExecutorService executor;


    public SqoopBase(final PluginDAO pluginDAO)
    {
        this.pluginDAO = pluginDAO;
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


    public PluginDAO getPluginDao()
    {
        return pluginDAO;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( ExecutorService executor )
    {
        this.executor = executor;
    }

}

