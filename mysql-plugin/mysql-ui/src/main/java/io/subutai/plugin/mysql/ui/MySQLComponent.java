package io.subutai.plugin.mysql.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.ui.environment.EnvironmentWizard;
import io.subutai.plugin.mysql.ui.manager.Manager;


public class MySQLComponent extends CustomComponent
{
    private final Manager manager;


    public MySQLComponent( final ExecutorService executorService, final MySQLC mySQLC, final Tracker tracker,
                           final EnvironmentManager environmentManager ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        setCompositionRoot( verticalLayout );
        TabSheet sheet = new TabSheet();
        manager = new Manager( executorService, mySQLC, tracker, environmentManager );
        final EnvironmentWizard environmentWizard =
                new EnvironmentWizard( executorService, mySQLC, tracker, environmentManager );

        sheet.addTab( environmentWizard.getContent(), "Install" );
        sheet.getTab( 0 ).setId( "MySQLInstallTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "MySQLManageTab" );

        sheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {

            @Override
            public void selectedTabChange( final TabSheet.SelectedTabChangeEvent selectedTabChangeEvent )
            {
                TabSheet tabSheet = selectedTabChangeEvent.getTabSheet();
                String caption = tabSheet.getTab( selectedTabChangeEvent.getTabSheet().getSelectedTab() ).getCaption();
                if ( caption.equals( "Manage" ) )
                {
                    manager.refreshClusterInfo();
                }
            }
        } );

        verticalLayout.addComponent( sheet );
        manager.refreshClusterInfo();
    }
}
