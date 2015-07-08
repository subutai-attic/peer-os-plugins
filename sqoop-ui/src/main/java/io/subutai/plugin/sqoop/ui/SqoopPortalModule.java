package io.subutai.plugin.sqoop.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.sqoop.api.Sqoop;
import io.subutai.plugin.sqoop.api.SqoopConfig;
import io.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class SqoopPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "sqoop.png";
    protected static final Logger LOG = Logger.getLogger( SqoopPortalModule.class.getName() );
    private ExecutorService executor;
    private final Sqoop sqoop;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private Hadoop hadoop;


    public SqoopPortalModule( Sqoop sqoop, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.sqoop = sqoop;
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
        return SqoopConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return SqoopConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( SqoopPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new SqoopComponent( executor, sqoop, hadoop, tracker, environmentManager );
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
