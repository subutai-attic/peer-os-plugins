package io.subutai.plugin.flume.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import com.vaadin.ui.Component;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.server.ui.api.PortalModule;


public class FlumePortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "flume.png";
    protected static final Logger LOG = Logger.getLogger( FlumePortalModule.class.getName() );
    private ExecutorService executor;
    private Flume flume;
    private Hadoop hadoop;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public FlumePortalModule( Flume flume, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.flume = flume;
        this.hadoop = hadoop;
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
        return FlumeConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return FlumeConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( FlumePortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new FlumeComponent( executor, flume, hadoop, tracker, environmentManager );
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
