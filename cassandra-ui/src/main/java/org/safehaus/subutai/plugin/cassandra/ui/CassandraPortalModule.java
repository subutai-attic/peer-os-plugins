/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.cassandra.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.naming.NamingException;

import org.safehaus.subutai.common.mdc.SubutaiExecutors;
import org.safehaus.subutai.common.util.FileUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.cassandra.api.Cassandra;
import org.safehaus.subutai.plugin.cassandra.api.CassandraClusterConfig;
import org.safehaus.subutai.server.ui.api.PortalModule;

import com.vaadin.ui.Component;


public class CassandraPortalModule implements PortalModule
{

    public static final String MODULE_IMAGE = "cassandra.png";
    protected Logger LOG = Logger.getLogger( CassandraPortalModule.class.getName() );
    private ExecutorService executor;
    private Cassandra cassandra;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public CassandraPortalModule( Cassandra cassandra, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.cassandra = cassandra;
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
        return CassandraClusterConfig.PRODUCT_KEY;
    }


    @Override
    public String getName()
    {
        return CassandraClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( CassandraPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        try
        {
            return new CassandraComponent( executor, cassandra, tracker, environmentManager );
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
