package io.subutai.plugin.oozie.ui.wizard;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;

import com.vaadin.ui.Component;
import com.vaadin.ui.VerticalLayout;


public class Wizard
{

    private final VerticalLayout vlayout;
    private final ExecutorService executor;
    private final Oozie oozie;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private int step = 1;
    private OozieClusterConfig config = new OozieClusterConfig();
    private Hadoop hadoop;


    public Wizard(final ExecutorService executorService, Oozie oozie, Hadoop hadoop, Tracker tracker,
                  EnvironmentManager environmentManager) throws NamingException
    {

//        tracker = serviceLocator.getService( Tracker.class );
        this.hadoop = hadoop;
        this.oozie = oozie;
        this.executor = executorService;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        vlayout = new VerticalLayout();
        vlayout.setSizeFull();
        vlayout.setMargin(true);
        putForm();
    }


    private void putForm()
    {
        vlayout.removeAllComponents();
        switch (step)
        {
            case 1:
            {
                vlayout.addComponent(new StepStart(this));
                break;
            }
            case 2:
            {
                vlayout.addComponent(new ConfigurationStep(this));
                break;
            }
            case 3:
            {
                try
                {
                    vlayout.addComponent(new StepSetConfig(oozie, hadoop, this, environmentManager));
                }
                catch ( EnvironmentNotFoundException e )
                {
                    e.printStackTrace();
                }
                break;
            }
            case 4:
            {
                vlayout.addComponent(new VerificationStep(this, environmentManager));
                break;
            }
            default:
            {
                step = 1;
                vlayout.addComponent(new StepStart(this));
                break;
            }
        }
    }


    public Component getContent()
    {
        return vlayout;
    }


    public void next()
    {
        step++;
        putForm();
    }


    public void back()
    {
        step--;
        putForm();
    }


    public void cancel()
    {
        step = 1;
        putForm();
    }


    public void init()
    {
        step = 1;
        config = new OozieClusterConfig();
        putForm();
    }


    public OozieClusterConfig getConfig()
    {
        return config;
    }


    public Hadoop getHadoopManager()
    {
        return hadoop;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public Oozie getOozieManager()
    {
        return oozie;
    }
}
