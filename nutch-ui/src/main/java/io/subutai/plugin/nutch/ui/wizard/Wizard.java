package io.subutai.plugin.nutch.ui.wizard;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.nutch.api.Nutch;
import io.subutai.plugin.nutch.api.NutchConfig;


public class Wizard
{

    private final GridLayout grid;
    private final ExecutorService executorService;
    private final Hadoop hadoop;
    private final Nutch nutch;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private int step = 1;
    private NutchConfig config = new NutchConfig();


    public Wizard( ExecutorService executorService, Nutch nutch, Hadoop hadoop, Tracker tracker,
                   EnvironmentManager environmentManager ) throws NamingException
    {

        this.executorService = executorService;
        this.nutch = nutch;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        grid = new GridLayout( 1, 20 );
        grid.setMargin( true );
        grid.setSizeFull();

        putForm();
    }


    private void putForm()
    {
        grid.removeComponent( 0, 1 );
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
                component = new ConfigurationStep( hadoop, this, environmentManager );
                break;
            }
            case 3:
            {
                component = new VerificationStep( nutch, executorService, tracker, this );
                break;
            }
            default:
            {
                break;
            }
        }

        if ( component != null )
        {
            grid.addComponent( component, 0, 1, 0, 19 );
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
        config = new NutchConfig();
        putForm();
    }


    public NutchConfig getConfig()
    {
        return config;
    }


    public Nutch getNutchManager()
    {
        return nutch;
    }
}
