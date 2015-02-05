package org.safehaus.subutai.plugin.storm.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.ui.environment.EnvironmentWizard;
import org.safehaus.subutai.plugin.storm.ui.manager.Manager;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class StormComponent extends CustomComponent
{

    private final Manager manager;
    private final EnvironmentWizard environmentWizard;


    public StormComponent( ExecutorService executorService, Storm storm, Zookeeper zookeeper,  Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {
        manager = new Manager( executorService, storm, zookeeper, tracker, environmentManager );
        environmentWizard = new EnvironmentWizard( executorService, storm, zookeeper, tracker, environmentManager );

        setSizeFull();
        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet tabSheet = new TabSheet();
        tabSheet.setSizeFull();
        tabSheet.addTab( environmentWizard.getContent(), "Install" );
        tabSheet.getTab( 0 ).setId( "StormConfigureEnvironmentTab" );
        tabSheet.addTab( manager.getContent(), "Manage" );
        tabSheet.getTab( 1 ).setId( "StormManageTab" );

        verticalLayout.addComponent( tabSheet );
        setCompositionRoot( verticalLayout );
        tabSheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
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
    }
}
