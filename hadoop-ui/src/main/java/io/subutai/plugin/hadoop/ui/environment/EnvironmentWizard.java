package io.subutai.plugin.hadoop.ui.environment;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import com.vaadin.ui.Component;
import com.vaadin.ui.VerticalLayout;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.hostregistry.api.HostRegistry;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class EnvironmentWizard
{
    private final VerticalLayout grid;
    private final Hadoop hadoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private int step = 1;
    private HadoopClusterConfig hadoopClusterConfig = new HadoopClusterConfig();
    private HostRegistry hostRegistry;
    private EnvironmentManager environmentManager;


    public EnvironmentWizard( ExecutorService executorService, Hadoop hadoop, HostRegistry hostRegistry,
                              Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {

        this.tracker = tracker;
        this.executorService = executorService;
        this.hadoop = hadoop;
        this.hostRegistry = hostRegistry;
        this.environmentManager = environmentManager;


        grid = new VerticalLayout();
        grid.setMargin( true );
        grid.setSizeFull();

        putForm();
    }


    private void putForm()
    {
        grid.removeAllComponents();
        Component component = null;
        switch ( step )
        {
            case 1:
            {
                component = new WelcomeStep( this );
                break;
            }
            case 2:
            {
                component = new ConfigurationStep( this, hostRegistry, environmentManager );

                break;
            }
            case 3:
            {
                component = new VerificationStep( hadoop, executorService, tracker, this );
                break;
            }
            default:
            {
                break;
            }
        }

        if ( component != null )
        {
            grid.addComponent( component );
        }
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


    protected void init()
    {
        step = 1;
        hadoopClusterConfig = new HadoopClusterConfig();
        putForm();
    }


    public void clearConfig()
    {
        hadoopClusterConfig = new HadoopClusterConfig();
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public HadoopClusterConfig getHadoopClusterConfig()
    {
        return hadoopClusterConfig;
    }


    public Hadoop getHadoopManager()
    {
        return hadoop;
    }
}
