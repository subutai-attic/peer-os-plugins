package io.subutai.plugin.storm.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.ui.manager.Manager;
import io.subutai.plugin.storm.ui.wizard.Wizard;
import io.subutai.plugin.zookeeper.api.Zookeeper;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class StormComponent extends CustomComponent
{

    private final Manager manager;
    private final Wizard wizard;


    public StormComponent( ExecutorService executorService, Storm storm, Zookeeper zookeeper, Tracker tracker,
                           EnvironmentManager environmentManager ) throws NamingException
    {
        manager = new Manager( executorService, storm, zookeeper, tracker, environmentManager );
        wizard = new Wizard( executorService, storm, zookeeper, tracker, environmentManager );

        setSizeFull();
        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet tabSheet = new TabSheet();
        tabSheet.setSizeFull();
        tabSheet.addTab( wizard.getContent(), "Install" );
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
