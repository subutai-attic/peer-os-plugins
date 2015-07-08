package io.subutai.plugin.zookeeper.ui;


import java.io.File;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.util.FileUtil;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.server.ui.api.PortalModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.ui.Component;


public class ZookeeperPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "zookeeper.png";
    protected static final Logger LOG = LoggerFactory.getLogger( ZookeeperPortalModule.class.getName() );
    private ExecutorService executor;
    private final Hadoop hadoop;
    private final Zookeeper zookeeper;
    private final EnvironmentManager environmentManager;
    private final Tracker tracker;


    public ZookeeperPortalModule( Zookeeper zookeeper, Hadoop hadoop, Tracker tracker,
                                  EnvironmentManager environmentManager )
    {
        this.zookeeper = zookeeper;
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
        return ZookeeperClusterConfig.PRODUCT_KEY;
    }


    public String getName()
    {
        return ZookeeperClusterConfig.PRODUCT_KEY;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( ZookeeperPortalModule.MODULE_IMAGE, this );
    }


    public Component createComponent()
    {
        try
        {
            return new ZookeeperComponent( executor, zookeeper, hadoop, tracker, environmentManager );
        }
        catch ( NamingException e )
        {
            LOG.error( e.getMessage() );
        }
        return null;
    }


    @Override
    public Boolean isCorePlugin()
    {
        return false;
    }
}
