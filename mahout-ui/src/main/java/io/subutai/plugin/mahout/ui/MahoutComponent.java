/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.ui.manager.Manager;
import io.subutai.plugin.mahout.ui.wizard.Wizard;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class MahoutComponent extends CustomComponent
{

    public MahoutComponent( ExecutorService executorService, Mahout mahout, Hadoop hadoop, Tracker tracker,
                            EnvironmentManager environmentManager ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet sheet = new TabSheet();
        sheet.setSizeFull();

        final Manager manager = new Manager( executorService, mahout, tracker, hadoop, environmentManager );
        Wizard wizard = new Wizard( executorService, mahout, hadoop, tracker, environmentManager );
        sheet.addTab( wizard.getContent(), "Install" );
        sheet.getTab( 0 ).setId( "MahoutInstallTab" );
        sheet.addTab( manager.getContent(), "Manage" );
        sheet.getTab( 1 ).setId( "MahoutManageTab" );
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

        setCompositionRoot( verticalLayout );
        manager.refreshClustersInfo();
    }
}

