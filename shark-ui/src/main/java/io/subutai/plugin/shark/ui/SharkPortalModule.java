package io.subutai.plugin.shark.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.shark.api.Shark;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class SharkPortalModule implements PortalModule
{

    public static final String MODULE_IMAGE = "shark.png";
    protected static final Logger LOG = Logger.getLogger( SharkPortalModule.class.getName() );
    private ExecutorService executor;
    private final Spark spark;
    private final Tracker tracker;
    private final Shark shark;
    private final EnvironmentManager environmentManager;


    public SharkPortalModule( Shark shark, Spark spark, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.shark = shark;
        this.spark = spark;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public String getId()
    {
        return SharkClusterConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return SharkClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( SharkPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new SharkComponent( executor, shark, spark, tracker, environmentManager );
        }
        catch ( NamingException e )
        {
            LOG.severe( e.getMessage() );
        }
        return null;
    }


    @Override
    public Boolean isCorePlugin()
    {
        return false;
    }
}

