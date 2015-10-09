package io.subutai.plugin.hadoop.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.hostregistry.api.HostRegistry;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.ui.environment.EnvironmentWizard;
import io.subutai.plugin.hadoop.ui.manager.Manager;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class HadoopComponent extends CustomComponent
{
    private final Manager manager;
    final EnvironmentWizard environmentWizard;

    public HadoopComponent( ExecutorService executorService, Tracker tracker, Hadoop hadoop, EnvironmentManager environmentManager, HostRegistry hostRegistry ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet sheet = new TabSheet();
        sheet.setSizeFull();

        manager = new Manager( executorService, tracker, hadoop, environmentManager );
        environmentWizard =
                new EnvironmentWizard( executorService, hadoop, hostRegistry, tracker, environmentManager );

        sheet.addTab( environmentWizard.getContent(), "Install" );
        sheet.getTab( 0 ).setId( "HadoopEnvironmentTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "HadoopManageTab" );
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
                    manager.getCheckAllButton().click();
                }
            }
        } );
        verticalLayout.addComponent( sheet );
        setCompositionRoot( verticalLayout );
    }
}
