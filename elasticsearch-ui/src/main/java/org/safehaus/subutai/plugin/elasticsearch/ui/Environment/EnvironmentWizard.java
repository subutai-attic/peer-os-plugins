package org.safehaus.subutai.plugin.elasticsearch.ui.Environment;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.VerticalLayout;


public class EnvironmentWizard
{

    private final VerticalLayout verticalLayout;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final Elasticsearch cassandra;
    private EnvironmentManager environmentManager;
    private GridLayout grid;
    private int step = 1;
    private ElasticsearchClusterConfiguration config = new ElasticsearchClusterConfiguration();


    public EnvironmentWizard( ExecutorService executorService, Elasticsearch cassandra, Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {

        this.executorService = executorService;
        this.cassandra = cassandra;
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
                verticalLayout.addComponent( new ConfigurationStep( this ) );
                break;
            }
            case 3:
            {
                verticalLayout.addComponent( new VerificationStep( cassandra, executorService, tracker, this ) );
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


    public ElasticsearchClusterConfiguration getConfig()
    {
        return config;
    }


    public void clearConfig(){
        config = new ElasticsearchClusterConfiguration();
    }


    public void init()
    {
        step = 1;
        config = new ElasticsearchClusterConfiguration();
        putForm();
    }
}
