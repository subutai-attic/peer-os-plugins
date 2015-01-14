package org.safehaus.subutai.plugin.elasticsearch.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;
import org.safehaus.subutai.plugin.elasticsearch.ui.wizard.EnvironmentWizard;
import org.safehaus.subutai.plugin.elasticsearch.ui.manager.Manager;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class ElasticsearchComponent extends CustomComponent
{
    private final Manager manager;
    private final EnvironmentWizard environmentWizard;


    public ElasticsearchComponent( ExecutorService executorService, Elasticsearch elasticsearch, Tracker tracker,
                                   EnvironmentManager environmentManager ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        setCompositionRoot( verticalLayout );
        TabSheet sheet = new TabSheet();
        sheet.setSizeFull();
        manager = new Manager( executorService, elasticsearch, tracker, environmentManager );
        environmentWizard = new EnvironmentWizard( executorService, elasticsearch, tracker, environmentManager );


        sheet.addTab( environmentWizard.getContent(), "Setup" );
        sheet.getTab( 0 ).setId( "ElasticSearchConfigureEnvironmentTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "ElasticSearchManageTab" );
        sheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {
            @Override
            public void selectedTabChange( TabSheet.SelectedTabChangeEvent event )
            {
                TabSheet tabsheet = event.getTabSheet();
                String caption = tabsheet.getTab( event.getTabSheet().getSelectedTab() ).getCaption();
                if ( "Manage".equals( caption ) )
                {
                    manager.refreshClustersInfo();
                }
            }
        } );
        verticalLayout.addComponent( sheet );
        manager.refreshClustersInfo();
    }
}
