package io.subutai.plugin.hive.ui.wizard;


import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.hive.api.HiveConfig;
import io.subutai.server.ui.component.ProgressWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Window;


public class VerificationStep extends Panel
{
    private static final Logger LOGGER = LoggerFactory.getLogger( VerificationStep.class );

    public VerificationStep( final Hive hive, final Hadoop hadoop, final ExecutorService executorService,
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

        final HiveConfig config = wizard.getConfig();
        final HadoopClusterConfig hc = hadoop.getCluster( config.getHadoopClusterName() );
        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Installation name", config.getClusterName() );

        Environment hadoopEnvironment = null;
        try
        {
            hadoopEnvironment = environmentManager.findEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Environment not found.", e );
        }
        ContainerHost master = null;
        try
        {
            master = hadoopEnvironment.getContainerHostById( wizard.getConfig().getServer() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        Set<ContainerHost> slaves = null;
        try
        {
            slaves = hadoopEnvironment.getContainerHostsByIds( wizard.getConfig().getClients() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        cfgView.addStringCfg( "Hadoop cluster Name", wizard.getConfig().getHadoopClusterName() );
        cfgView.addStringCfg( "Server node", master.getHostname() );
        for ( ContainerHost slave : slaves )
        {
            cfgView.addStringCfg( "Node(s) to install", slave.getHostname() );
        }


        Button install = new Button( "Install" );
        install.setId( "HiveVerInstall" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackId = hive.installCluster( config );

                ProgressWindow window = new ProgressWindow( executorService, tracker, trackId, HiveConfig.PRODUCT_KEY );
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
        back.setId( "HiveVerBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                wizard.clearConfig();
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
