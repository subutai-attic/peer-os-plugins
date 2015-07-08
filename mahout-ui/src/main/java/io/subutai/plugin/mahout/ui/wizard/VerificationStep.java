/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.ui.wizard;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.server.ui.component.ProgressWindow;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Window;


public class VerificationStep extends Panel
{

    public VerificationStep( final Hadoop hadoop, final Mahout mahout, final ExecutorService executorService,
                             final Tracker tracker, final EnvironmentManager environmentManager, final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(you may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        final MahoutClusterConfig config = wizard.getConfig();
        final HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );

        if ( hadoopClusterConfig == null )
        {
            Notification.show( String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
            return;
        }

        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Installation Name", wizard.getConfig().getClusterName() );
        cfgView.addStringCfg( "Hadoop cluster name", wizard.getConfig().getHadoopClusterName() );


        Set<ContainerHost> nodes;
        try
        {
            nodes = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() )
                                      .getContainerHostsByIds( wizard.getConfig().getNodes() );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            Notification.show( String.format( "Error accessing environment: %s", e ) );
            return;
        }

        for ( ContainerHost host : nodes )
        {
            cfgView.addStringCfg( "Node to install", host.getHostname() + "" );
        }


        Button install = new Button( "Install" );
        install.setId( "MahoutVerificationInstall" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackId = mahout.installCluster( config );

                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackId, MahoutClusterConfig.PRODUCT_KEY );
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
        back.setId( "MahoutVerificationBack" );
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

        setContent( grid );
    }
}
