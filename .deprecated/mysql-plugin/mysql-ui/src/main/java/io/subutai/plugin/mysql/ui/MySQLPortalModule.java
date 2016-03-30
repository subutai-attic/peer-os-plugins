package io.subutai.plugin.mysql.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.naming.NamingException;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class MySQLPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "mysql.png";
    protected Logger LOG = Logger.getLogger( MySQLPortalModule.class.getName() );

    private ExecutorService executorService;
    private MySQLC mySQLC;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public MySQLPortalModule( MySQLC mysqlc, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.mySQLC = mysqlc;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        LOG.warning( "Creating: " + MySQLPortalModule.class.toString() );
    }


    public void init()
    {
        executorService = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executorService.shutdown();
    }


    @Override
    public String getId()
    {
        return MySQLClusterConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return MySQLClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( MySQLPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {

            return new MySQLComponent( executorService, mySQLC, tracker, environmentManager );
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
