package io.subutai.plugin.cassandra.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.Cassandra;
import io.subutai.plugin.cassandra.ui.environment.EnvironmentWizard;
import io.subutai.plugin.cassandra.ui.manager.Manager;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class CassandraComponent extends CustomComponent
{

    private final Manager manager;


    public CassandraComponent( ExecutorService executorService, Cassandra cassandra, Tracker tracker,
                               EnvironmentManager environmentManager ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        setCompositionRoot( verticalLayout );

        TabSheet sheet = new TabSheet();
        sheet.setSizeFull();

        manager = new Manager( executorService, cassandra, tracker, environmentManager );
        final EnvironmentWizard environmentWizard =
                new EnvironmentWizard( executorService, cassandra, tracker, environmentManager );
        sheet.addTab( environmentWizard.getContent(), "Install" );
        sheet.getTab( 0 ).setId( "CassandraInstallTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "CassandraManageTab" );
        sheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {
            @Override
            public void selectedTabChange( TabSheet.SelectedTabChangeEvent event )
            {
                TabSheet tabsheet = event.getTabSheet();
                String caption = tabsheet.getTab( event.getTabSheet().getSelectedTab() ).getCaption();
                if ( caption.equals( "Manage" ) )
                {
                    manager.refreshClustersInfo();
                }
            }
        } );
        verticalLayout.addComponent( sheet );
        manager.refreshClustersInfo();
    }
}
