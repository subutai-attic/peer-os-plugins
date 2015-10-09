/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mongodb.ui.wizard;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.mongodb.api.Mongo;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.server.ui.component.ProgressWindow;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


public class EnvironmentVerificationStep extends VerticalLayout
{

    public EnvironmentVerificationStep( final Mongo mongo, final ExecutorService executorService, final Tracker tracker,
                                        final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(you may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Cluster Name", wizard.getMongoClusterConfig().getClusterName() );
        cfgView.addStringCfg( "Replica Set Name", wizard.getMongoClusterConfig().getReplicaSetName() );
        cfgView.addStringCfg( "Domain Name", wizard.getMongoClusterConfig().getDomainName() );

        cfgView.addStringCfg( "Number of configuration servers",
                wizard.getMongoClusterConfig().getNumberOfConfigServers() + "" );
        String serverNames = "";
        for ( final String configNodeName : wizard.getConfigServerNames() )
        {
            serverNames += configNodeName + ";";
        }
        cfgView.addStringCfg( "Configuration servers", serverNames );

        cfgView.addStringCfg( "Number of routers", wizard.getMongoClusterConfig().getNumberOfRouters() + "" );
        serverNames = "";
        for ( final String routerNodeName : wizard.getRouterServerNames() )
        {
            serverNames += routerNodeName + ";";
        }
        cfgView.addStringCfg( "Router servers", serverNames );

        cfgView.addStringCfg( "Number of data nodes", wizard.getMongoClusterConfig().getNumberOfDataNodes() + "" );
        serverNames = "";
        for ( final String dataNodeName : wizard.getDataServerNames() )
        {
            serverNames += dataNodeName + ";";
        }
        cfgView.addStringCfg( "Data nodes", serverNames );
        cfgView.addStringCfg( "Configuration servers port", wizard.getMongoClusterConfig().getCfgSrvPort() + "" );
        cfgView.addStringCfg( "Routers port", wizard.getMongoClusterConfig().getRouterPort() + "" );
        cfgView.addStringCfg( "Data nodes port", wizard.getMongoClusterConfig().getDataNodePort() + "" );

        Button install = new Button( "Install" );
        install.setId( "install" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackID = mongo.installCluster( wizard.getMongoClusterConfig() );
                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackID, MongoClusterConfig.PRODUCT_KEY );
                window.getWindow().addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
                        wizard.init();
                    }
                } );
                getUI().addWindow( window.getWindow() );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "envVerBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                wizard.back();
            }
        } );

        HorizontalLayout buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( install );

        grid.addComponent( confirmationLbl, 0, 0 );

        grid.addComponent( cfgView.getCfgTable(), 0, 1, 0, 3 );

        grid.addComponent( buttons, 0, 4 );

        addComponent( grid );
    }
}
