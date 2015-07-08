package io.subutai.plugin.storm.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class StormPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "storm.png";
    protected static final Logger LOG = Logger.getLogger( StormPortalModule.class.getName() );
    private final Storm storm;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private ExecutorService executor;
    private Zookeeper zookeeper;


    public StormPortalModule( Storm storm, Zookeeper zookeeper, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.storm = storm;
        this.zookeeper = zookeeper;
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
        return StormClusterConfiguration.PRODUCT_NAME;
    }


    @Override
    public String getName()
    {
        return StormClusterConfiguration.PRODUCT_NAME;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( StormPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new StormComponent( executor, storm, zookeeper, tracker, environmentManager );
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
