package io.subutai.plugin.mysql.ui.environment;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.server.ui.component.ProgressWindow;


public class VerificationStep extends VerticalLayout
{
    public VerificationStep( final MySQLC mySQLC, final ExecutorService executorService, final Tracker tracker,
                             final EnvironmentWizard environmentWizard )
    {
        setSizeFull();
        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(You may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Cluster Name", environmentWizard.getConfig().getClusterName() );
        cfgView.addStringCfg( "Domain Name", environmentWizard.getConfig().getDomainName() );
        cfgView.addStringCfg( "Data Node Config File", environmentWizard.getConfig().getConfNodeDataFile() );
        cfgView.addStringCfg( "Data Node Data Dir", environmentWizard.getConfig().getDataNodeDataDir() );
        cfgView.addStringCfg( "Manager Node Config File", environmentWizard.getConfig().getConfManNodeFile() );
        cfgView.addStringCfg( "Manager Node Data Dir", environmentWizard.getConfig().getDataManNodeDir() );

        Environment environment;
        String selectedNodes = "";
        String seeds = "";
        try
        {
            environment = environmentWizard.getEnvironmentManager()
                                           .loadEnvironment( environmentWizard.getConfig().getEnvironmentId() );
            for ( String uuid : environmentWizard.getConfig().getManagerNodes() )
            {
                selectedNodes += environment.getContainerHostById( uuid ).getHostname() + ",";
            }
            for ( String uuid : environmentWizard.getConfig().getDataNodes() )
            {
                seeds += environment.getContainerHostById( uuid ).getHostname() + ",";
            }
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            Notification.show( e.getMessage() );
            return;
        }
        cfgView.addStringCfg( "Manager nodes to be configured",
                selectedNodes.substring( 0, ( selectedNodes.length() - 1 ) ) );
        cfgView.addStringCfg( "Data nodes", seeds.substring( 0, ( seeds.length() - 1 ) ) + "" );
        cfgView.addStringCfg( "Environment UUID", environmentWizard.getConfig().getEnvironmentId() + "" );
        Button install = new Button( "Configure" );
        install.setId( "SqlConfigInstallBtn" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackID = mySQLC.installCluster( environmentWizard.getConfig() );
                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackID, MySQLClusterConfig.PRODUCT_KEY );
                window.getWindow().addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
                        environmentWizard.init();
                    }
                } );
                getUI().addWindow( window.getWindow() );
            }
        } );
        Button back = new Button( "Back" );
        back.setId( "SqlVerBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                environmentWizard.clearConfig();
                environmentWizard.back();
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
