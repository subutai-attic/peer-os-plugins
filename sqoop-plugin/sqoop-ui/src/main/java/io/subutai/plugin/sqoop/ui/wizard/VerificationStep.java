package io.subutai.plugin.sqoop.ui.wizard;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Sets;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Window;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.sqoop.api.Sqoop;
import io.subutai.plugin.sqoop.api.SqoopConfig;
import io.subutai.server.ui.component.ProgressWindow;


public class VerificationStep extends Panel
{

    public VerificationStep( final Sqoop sqoop, final Hadoop hadoop, final ExecutorService executorService,
                             final Tracker tracker, EnvironmentManager environmentManager, final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(you may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        final SqoopConfig config = wizard.getConfig();
        final HadoopClusterConfig hc = hadoop.getCluster( wizard.getConfig().getHadoopClusterName() );
        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Installation name", wizard.getConfig().getClusterName() );

        Environment hadoopEnv = null;
        try
        {
            hadoopEnv = environmentManager.loadEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        Set<EnvironmentContainerHost> hosts = Sets.newHashSet();
        if ( hadoopEnv != null )
        {
            try
            {
                hosts = hadoopEnv.getContainerHostsByIds( config.getNodes() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        for ( EnvironmentContainerHost host : hosts )
        {
            cfgView.addStringCfg( "Node(s) to install", host.getHostname() );
        }

        Button install = new Button( "Install" );
        install.setId( "sqoopVerificationInstall" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                UUID trackId;
                trackId = sqoop.installCluster( wizard.getConfig() );
                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackId, SqoopConfig.PRODUCT_KEY );
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
        back.setId( "sqoopVerificationBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
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

