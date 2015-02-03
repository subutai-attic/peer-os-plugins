package org.safehaus.subutai.plugin.lucene.ui.wizard;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.ui.ConfigView;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.lucene.api.Lucene;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;
import org.safehaus.subutai.server.ui.component.ProgressWindow;

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

    public VerificationStep( final Hadoop hadoop, final Lucene lucene, final ExecutorService executorService,
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

        // Display config values

        final LuceneConfig config = wizard.getConfig();
        final HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
        if ( hadoopClusterConfig == null )
        {
            Notification.show( String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
            return;
        }

        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Hadoop cluster name", config.getHadoopClusterName() );

        Set<ContainerHost> nodes;
        try
        {
            nodes = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() )
                                      .getContainerHostsByIds( wizard.getConfig().getNodes() );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            Notification.show( String.format( "Error accessing Hadoop environment: %s", e ) );
            return;
        }

        for ( ContainerHost host : nodes )
        {
            cfgView.addStringCfg( "Node to install", host.getHostname() + "" );
        }


        // Install button

        Button install = new Button( "Install" );
        install.setId( "LuceneVerInstall" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {

                UUID trackId = lucene.installCluster( config );

                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackId, LuceneConfig.PRODUCT_KEY );
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
        back.setId( "LuceneVerBack" );
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
