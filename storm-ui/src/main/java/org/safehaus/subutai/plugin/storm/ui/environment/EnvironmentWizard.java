package org.safehaus.subutai.plugin.storm.ui.environment;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.VerticalLayout;


public class EnvironmentWizard
{

    private final VerticalLayout verticalLayout;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final Storm storm;
    private final Zookeeper zookeeper;
    private EnvironmentManager environmentManager;
    private GridLayout grid;
    private int step = 1;
    private StormClusterConfiguration config = new StormClusterConfiguration();
    private ZookeeperClusterConfig zookeeperClusterConfig = new ZookeeperClusterConfig();


    public EnvironmentWizard( ExecutorService executorService, Storm storm, Zookeeper zookeeper, Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {

        this.executorService = executorService;
        this.storm = storm;
        this.zookeeper = zookeeper;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        verticalLayout = new VerticalLayout();
        verticalLayout.setSizeFull();
        grid = new GridLayout( 1, 1 );
        grid.setMargin( true );
        grid.setSizeFull();
        grid.addComponent( verticalLayout );
        grid.setComponentAlignment( verticalLayout, Alignment.TOP_CENTER );

        putForm();
    }


    private void putForm()
    {
        verticalLayout.removeAllComponents();
        switch ( step )
        {
            case 1:
            {
                verticalLayout.addComponent( new StepStart( this ) );
                break;
            }
            case 2:
            {
                verticalLayout.addComponent( new ConfigurationStep( this, zookeeper ) );
                break;
            }
            case 3:
            {
                verticalLayout.addComponent( new VerificationStep( storm, executorService, tracker, this ) );
                break;
            }
            default:
            {
                step = 1;
                verticalLayout.addComponent( new StepStart( this ) );
                break;
            }
        }
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public Component getContent()
    {
        return grid;
    }


    protected void next()
    {
        step++;
        putForm();
    }


    protected void back()
    {
        step--;
        putForm();
    }


    protected void cancel()
    {
        step = 1;
        putForm();
    }


    public ZookeeperClusterConfig getZookeeperClusterConfig()
    {
        return zookeeperClusterConfig;
    }


    public void setZookeeperClusterConfig( final ZookeeperClusterConfig zookeeperClusterConfig )
    {
        this.zookeeperClusterConfig = zookeeperClusterConfig;
    }


    public StormClusterConfiguration getConfig()
    {
        return config;
    }


    public void clearConfig(){
        config = new StormClusterConfiguration();
    }


    protected void init( boolean externalZookeeper )
    {
        step = 1;
        config = new StormClusterConfiguration();
        config.setExternalZookeeper( externalZookeeper );
        putForm();
    }
}
