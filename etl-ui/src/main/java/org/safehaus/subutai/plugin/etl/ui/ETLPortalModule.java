package org.safehaus.subutai.plugin.etl.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.naming.NamingException;

import org.safehaus.subutai.common.util.FileUtil;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hive.api.Hive;
import org.safehaus.subutai.plugin.pig.api.Pig;
import org.safehaus.subutai.plugin.sqoop.api.Sqoop;
import org.safehaus.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class ETLPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "etl.png";
    protected static final Logger LOG = Logger.getLogger( ETLPortalModule.class.getName() );
    private ExecutorService executor;

    /** initialized by context.xml  */
    private final ETL etl;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private Hadoop hadoop;
    private Sqoop sqoop;
    private Hive hive;
    private Pig pig;


    public ETLPortalModule( ETL etl, Hadoop hadoop, Sqoop sqoop,
                            Hive hive, Pig pig, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.etl = etl;
        this.hadoop = hadoop;
        this.sqoop = sqoop;
        this.hive = hive;
        this.pig = pig;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    public void init()
    {
        executor = Executors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    @Override
    public String getId()
    {
        return ETLConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return ETLConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( ETLPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new ETLComponent( executor, etl, hadoop, hive, pig, sqoop, tracker, environmentManager );
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
