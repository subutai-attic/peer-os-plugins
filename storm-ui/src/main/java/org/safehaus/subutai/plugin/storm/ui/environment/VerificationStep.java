package org.safehaus.subutai.plugin.storm.ui.environment;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.server.ui.component.ProgressWindow;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


public class VerificationStep extends VerticalLayout
{

    public VerificationStep( final Storm storm, final ExecutorService executorService, final Tracker tracker,
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

        String selectedNodes = "";
        final Environment environment = environmentWizard.getEnvironmentManager().getEnvironmentByUUID(
                environmentWizard.getConfig().getEnvironmentId() );
        for ( UUID uuid : environmentWizard.getConfig().getSupervisors() ){
            selectedNodes += environment.getContainerHostById( uuid ).getHostname() + ",";
        }

        ContainerHost nimbusNodeNode = environment.getContainerHostById( environmentWizard.getConfig().getNimbus() );
        cfgView.addStringCfg( "Nodes to be configured as supervisor", selectedNodes.substring( 0, ( selectedNodes.length() - 1 ) ) );
        cfgView.addStringCfg( "Nimbus Node", nimbusNodeNode.getHostname() + "" );
        cfgView.addStringCfg( "Environment UUID", environmentWizard.getConfig().getEnvironmentId() + "" );

        Button configure = new Button( "Configure" );
        configure.setId( "StormVerificationInstall" );
        configure.addStyleName( "default" );
        configure.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackID = storm.configureEnvironmentCluster( environmentWizard.getConfig() );
                ProgressWindow window =
                        new ProgressWindow( executorService, tracker, trackID, StormClusterConfiguration.PRODUCT_KEY );
                window.getWindow().addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
//                        environmentWizard.init();
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
                environmentWizard.clearConfig();
                environmentWizard.back();
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
}
