/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.ui.manager.Manager;
import org.safehaus.subutai.plugin.mongodb.ui.wizard.Wizard;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class MongoComponent extends CustomComponent
{

    public MongoComponent( ExecutorService executorService, Mongo mongo, EnvironmentManager environmentManager,
                           Tracker tracker ) throws NamingException
    {
        setSizeFull();

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        TabSheet mongoSheet = new TabSheet();
        mongoSheet.setSizeFull();
        final Manager manager = new Manager( executorService, mongo, environmentManager, tracker );
        Wizard wizard = new Wizard( executorService, mongo, tracker, environmentManager );
        mongoSheet.addTab( wizard.getContent(), "Install" );
        mongoSheet.getTab( 0 ).setId( "InstallTab" );
        mongoSheet.addTab( manager.getContent(), "Manage" );
        mongoSheet.getTab( 1 ).setId( "ManageTab" );
        mongoSheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {
            @Override
            public void selectedTabChange( TabSheet.SelectedTabChangeEvent event )
            {
                TabSheet tabsheet = event.getTabSheet();
                String caption = tabsheet.getTab( event.getTabSheet().getSelectedTab() ).getCaption();
                if ( caption.equals( "Manage" ) )
                {
                    manager.refreshClustersInfo();
//                    manager.checkAllNodes();
                }
            }
        } );
        verticalLayout.addComponent( mongoSheet );

        setCompositionRoot( verticalLayout );
        manager.refreshClustersInfo();
    }
}
