package io.subutai.plugin.etl.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import com.vaadin.ui.Component;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.etl.api.ETL;
import io.subutai.plugin.etl.api.ETLConfig;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.pig.api.Pig;
import io.subutai.plugin.sqoop.api.Sqoop;
import io.subutai.server.ui.api.PortalModule;


public class ETLPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "etl.png";
    protected static final Logger LOG = Logger.getLogger( ETLPortalModule.class.getName() );
    private ExecutorService executor;

    /** initialized by context.xml */
    private final ETL etl;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private Hadoop hadoop;
    private Sqoop sqoop;
    private Hive hive;
    private Pig pig;


    public ETLPortalModule( ETL etl, Hadoop hadoop, Sqoop sqoop, Hive hive, Pig pig, Tracker tracker,
                            EnvironmentManager environmentManager )
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
        executor = SubutaiExecutors.newCachedThreadPool();
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
