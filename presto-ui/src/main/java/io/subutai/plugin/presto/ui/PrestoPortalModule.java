package io.subutai.plugin.presto.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import com.vaadin.ui.Component;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import io.subutai.server.ui.api.PortalModule;


public class PrestoPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "presto.png";
    protected static final Logger LOG = Logger.getLogger( PrestoPortalModule.class.getName() );
    private ExecutorService executor;
    private final Presto presto;
    private final Hadoop hadoop;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;


    public PrestoPortalModule( Presto presto, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.presto = presto;
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
        return PrestoClusterConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return PrestoClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( PrestoPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {

        try
        {
            return new PrestoComponent( executor, presto, hadoop, tracker, environmentManager );
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
