/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mahout.ui.wizard;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.mahout.api.Mahout;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;


public class Wizard
{

    private final GridLayout grid;
    private final Hadoop hadoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private final Mahout mahout;
    private int step = 1;
    private MahoutClusterConfig config = new MahoutClusterConfig();


    public Wizard( ExecutorService executorService, Mahout mahout, Hadoop hadoop, Tracker tracker,
                   EnvironmentManager environmentManager ) throws NamingException
    {

        this.executorService = executorService;
        this.mahout = mahout;
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
                component = new VerificationStep( hadoop, mahout, executorService, tracker, environmentManager, this );
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
        config = new MahoutClusterConfig();
        putForm();
    }


    public MahoutClusterConfig getConfig()
    {
        return config;
    }

    public Mahout getMahoutManager()
    {
        return mahout;
    }
}
