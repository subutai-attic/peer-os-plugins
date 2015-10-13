package io.subutai.plugin.pig.ui;


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
import io.subutai.plugin.pig.api.Pig;
import io.subutai.plugin.pig.api.PigConfig;
import io.subutai.server.ui.api.PortalModule;


public class PigPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "pig.png";
    protected static final Logger LOG = Logger.getLogger( PigPortalModule.class.getName() );
    private ExecutorService executor;
    private final Hadoop hadoop;
    private final Pig pig;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;


    public PigPortalModule( Pig pig, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.pig = pig;
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
        return PigConfig.PRODUCT_KEY;
    }


    public String getName()
    {
        return PigConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( PigPortalModule.MODULE_IMAGE, this );
    }


    public Component createComponent()
    {
        try
        {
            return new PigComponent( executor, pig, hadoop, tracker, environmentManager );
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
