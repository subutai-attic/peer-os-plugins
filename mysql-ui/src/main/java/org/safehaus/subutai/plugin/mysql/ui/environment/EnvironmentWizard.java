package org.safehaus.subutai.plugin.mysql.ui.environment;


import java.util.concurrent.ExecutorService;

import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLC;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.VerticalLayout;


public class EnvironmentWizard
{

    private final VerticalLayout verticalLayout;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final MySQLC mySQLC;
    private EnvironmentManager environmentManager;
    private GridLayout gridLayout;
    private int step = 1;

    private MySQLClusterConfig config = new MySQLClusterConfig();


    public EnvironmentWizard( ExecutorService executorService, MySQLC mysql, Tracker tracker,
                              EnvironmentManager environmentManager )
    {
        this.executorService = executorService;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.mySQLC = mysql;

        verticalLayout = new VerticalLayout();
        verticalLayout.setSizeFull();
        gridLayout = new GridLayout( 1, 1 );
        gridLayout.setMargin( true );
        gridLayout.setSizeFull();
        gridLayout.addComponent( verticalLayout );
        gridLayout.setComponentAlignment( verticalLayout, Alignment.TOP_CENTER );

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
                verticalLayout.addComponent( new ConfigurationStep( this ) );
                break;
            }
            case 3:
            {
                verticalLayout.addComponent( new VerificationStep( mySQLC, executorService, tracker, this ) );
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


    public MySQLClusterConfig getConfig()
    {
        return config;
    }


    public void clearConfig()
    {
        config = new MySQLClusterConfig();
    }


    public void init()
    {
        step = 1;
        config = new MySQLClusterConfig();
        putForm();
    }


    public Component getContent()
    {
        return gridLayout;
    }


    public MySQLC getMySQLManager()
    {
        return mySQLC;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }
}
