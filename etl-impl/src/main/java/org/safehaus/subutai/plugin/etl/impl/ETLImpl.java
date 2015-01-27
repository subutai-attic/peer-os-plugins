package org.safehaus.subutai.plugin.etl.impl;


import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.api.setting.ExportSetting;
import org.safehaus.subutai.plugin.etl.api.setting.ImportSetting;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ETLImpl implements ETL
{
    public static final Logger LOG = LoggerFactory.getLogger( ETLImpl.class );

    Tracker tracker;
    Hadoop hadoopManager;
    EnvironmentManager environmentManager;
    protected ExecutorService executor;

    public void init()
    {
        executor = Executors.newCachedThreadPool();
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


    @Override
    public UUID exportData( ExportSetting settings )
    {
        return null;
    }


    @Override
    public UUID importData( ImportSetting settings )
    {
        return null;
    }
}

