/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.ui;


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
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.server.ui.api.PortalModule;


public class MahoutPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "mahout.png";
    protected static final Logger LOG = Logger.getLogger( MahoutPortalModule.class.getName() );
    private ExecutorService executor;
    private Mahout mahout;
    private Hadoop hadoop;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public MahoutPortalModule( Mahout mahout, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.mahout = mahout;
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
        return MahoutClusterConfig.PRODUCT_KEY;
    }


    public String getName()
    {
        return MahoutClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( MahoutPortalModule.MODULE_IMAGE, this );
    }


    public Component createComponent()
    {
        try
        {
            return new MahoutComponent( executor, mahout, hadoop, tracker, environmentManager );
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
