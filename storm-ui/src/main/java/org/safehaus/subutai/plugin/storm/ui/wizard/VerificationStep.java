package org.safehaus.subutai.plugin.storm.ui.wizard;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.server.ui.component.ProgressWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

public class VerificationStep extends VerticalLayout
{
    private Environment environment;
    private EnvironmentManager environmentManager;
    private static final Logger LOGGER = LoggerFactory.getLogger( VerificationStep.class );


    public VerificationStep( final Storm storm, final ExecutorService executorService, final Tracker tracker,
                             final Wizard wizard, final EnvironmentManager environmentManager)
    {
        this.environmentManager = environmentManager;
        setSizeFull();

        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(You may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        StormClusterConfiguration config = wizard.getConfig();
        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Cluster Name", wizard.getConfig().getClusterName() );
        cfgView.addStringCfg( "Domain Name", wizard.getConfig().getDomainName() );
        if ( !config.isExternalZookeeper())
        {
            String selectedNodes = "";
            try
            {
                environment = wizard.getEnvironmentManager().findEnvironment( wizard.getConfig().getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment not found." );
                return;
            }
            for ( UUID uuid : wizard.getConfig().getSupervisors() )
            {
                try
                {
                    selectedNodes += environment.getContainerHostById( uuid ).getHostname() + ",";
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container host not found.", e );
                }
            }

            ContainerHost nimbusNodeNode = null;
            try
            {
                nimbusNodeNode = environment.getContainerHostById( wizard.getConfig().getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOGGER.error( "Container host not found.", e );
                return;
            }
            cfgView.addStringCfg( "Nodes to be configured as supervisor", selectedNodes.substring( 0, ( selectedNodes.length() - 1 ) ) );
            cfgView.addStringCfg( "Nimbus Node", nimbusNodeNode.getHostname() + "" );
            cfgView.addStringCfg( "Environment UUID", wizard.getConfig().getEnvironmentId() + "" );

            Button configure = new Button( "Configure" );
            configure.setId( "StormVerificationInstall" );
            configure.addStyleName( "default" );
            configure.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    UUID trackID = storm.configureEnvironmentCluster( wizard.getConfig() );
                    ProgressWindow window =
                            new ProgressWindow( executorService, tracker, trackID, StormClusterConfiguration.PRODUCT_KEY );
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
            back.setId( "StormVerificationBack" );
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
            buttons.addComponent( configure );

            grid.addComponent( confirmationLbl, 0, 0 );

            grid.addComponent( cfgView.getCfgTable(), 0, 1, 0, 3 );

            grid.addComponent( buttons, 0, 4 );

            addComponent( grid );
        }
        else
        {
            ZookeeperClusterConfig zookeeperClusterConfig = wizard.getZookeeperClusterConfig();
            Environment zookeeperEnvironment = null;
            try
            {
                zookeeperEnvironment = environmentManager.findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment not found " + zookeeperClusterConfig.getEnvironmentId().toString(), e );
                return;
            }
            ContainerHost nimbusHost = null;
            try
            {
                nimbusHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOGGER.error( "Container host not found " + config.getNimbus().toString(), e );
                return;
            }
            cfgView.addStringCfg( "Master node", nimbusHost.getHostname() );
            cfgView.addStringCfg( "Supervisor nodes count", config.getSupervisorsCount() + "" );

            Button install = new Button( "Install" );
            install.setId( "StormVerificationInstall" );
            install.addStyleName( "default" );
            install.addClickListener( new Button.ClickListener()
            {

                @Override
                public void buttonClick( Button.ClickEvent event )
                {

                    UUID trackID = storm.configureEnvironmentCluster( wizard.getConfig() );
                    ProgressWindow window =
                            new ProgressWindow( executorService, tracker, trackID, StormClusterConfiguration.PRODUCT_NAME );
                    window.getWindow().addCloseListener( new Window.CloseListener()
                    {
                        @Override
                        public void windowClose( Window.CloseEvent closeEvent )
                        {
                            wizard.init( );
                        }
                    } );
                    getUI().addWindow( window.getWindow() );
                }
            } );

            Button back = new Button( "Back" );
            back.setId( "StormVerificationBack" );
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

            addComponent( grid );
        }
    }
}