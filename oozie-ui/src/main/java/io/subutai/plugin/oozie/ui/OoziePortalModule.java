package io.subutai.plugin.oozie.ui;


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
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.server.ui.api.PortalModule;


public class OoziePortalModule implements PortalModule
{

    protected static final Logger LOG = Logger.getLogger( OoziePortalModule.class.getName() );
    public static final String MODULE_IMAGE = "oozie.png";
    private Oozie oozieManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;


    public OoziePortalModule( Oozie oozieManager, Hadoop hadoopManager, Tracker tracker,
                              EnvironmentManager environmentManager )
    {
        this.oozieManager = oozieManager;
        this.hadoopManager = hadoopManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    public void setOozieManager( final Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        oozieManager = null;
        tracker = null;
        hadoopManager = null;
        executor.shutdown();
    }


    @Override
    public String getId()
    {
        return OozieClusterConfig.PRODUCT_KEY;
    }


    public String getName()
    {
        return OozieClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( OoziePortalModule.MODULE_IMAGE, this );
    }


    public Component createComponent()
    {
        try
        {
            return new OozieComponent( executor, oozieManager, hadoopManager, tracker, environmentManager );
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
