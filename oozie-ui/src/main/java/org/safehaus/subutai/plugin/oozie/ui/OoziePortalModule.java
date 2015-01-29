package org.safehaus.subutai.plugin.oozie.ui;


import com.vaadin.ui.Component;
import org.safehaus.subutai.common.util.FileUtil;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.oozie.api.Oozie;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.server.ui.api.PortalModule;

import javax.naming.NamingException;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;


/**
 * @author dilshat
 */
public class OoziePortalModule implements PortalModule
{

    protected static final Logger LOG = Logger.getLogger( OoziePortalModule.class.getName() );
    public static final String MODULE_IMAGE = "oozie.png";
//    private final ServiceLocator serviceLocator;
    private Oozie oozieManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;

    public OoziePortalModule(Oozie oozieManager, Hadoop hadoopManager, Tracker tracker, EnvironmentManager environmentManager)
    {
        this.oozieManager = oozieManager;
        this.hadoopManager = hadoopManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
//        this.serviceLocator = new ServiceLocator();
    }


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( final Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void init()
    {
        executor = Executors.newCachedThreadPool();
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
            return new OozieComponent( executor, oozieManager, hadoopManager, tracker, environmentManager);
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
