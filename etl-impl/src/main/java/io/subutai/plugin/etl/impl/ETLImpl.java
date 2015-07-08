package io.subutai.plugin.etl.impl;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.etl.api.ETL;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.sqoop.api.Sqoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ETLImpl implements ETL
{
    public static final Logger LOG = LoggerFactory.getLogger( ETLImpl.class );

    Tracker tracker;
    Hadoop hadoopManager;
    Sqoop sqoop;
    EnvironmentManager environmentManager;
    protected ExecutorService executor;

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


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public Sqoop getSqoop()
    {
        return sqoop;
    }


    public void setSqoop( final Sqoop sqoop )
    {
        this.sqoop = sqoop;
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


    @Override
    public UUID isInstalled( final String clusterName, final String hostname )
    {
        return null;
    }
}

