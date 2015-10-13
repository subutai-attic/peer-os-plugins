package io.subutai.plugin.flume.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;

import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.ui.manager.Manager;
import io.subutai.plugin.flume.ui.wizard.Wizard;
import io.subutai.plugin.hadoop.api.Hadoop;


public class FlumeComponent extends CustomComponent
{


    public FlumeComponent( ExecutorService executorService, Flume flume, Hadoop hadoop, Tracker tracker,
                           EnvironmentManager environmentManager ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet sheet = new TabSheet();
        sheet.setSizeFull();
        final Manager manager = new Manager( executorService, flume, hadoop, tracker, environmentManager );
        final Wizard wizard = new Wizard( executorService, flume, hadoop, tracker, environmentManager );
        sheet.addTab( wizard.getContent(), "Install" );
        sheet.getTab( 0 ).setId( "FlumeInstallTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "FlumeManageTab" );
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
                    manager.checkAllNodes();
                }
            }
        } );

        verticalLayout.addComponent( sheet );
        setCompositionRoot( verticalLayout );
        manager.refreshClustersInfo();
    }
}
