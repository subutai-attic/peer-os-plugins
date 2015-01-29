package org.safehaus.subutai.plugin.oozie.ui.wizard;


import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.*;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.server.ui.component.ProgressWindow;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


public class VerificationStep extends Panel
{
    private EnvironmentManager environmentManager;

    public VerificationStep( final Wizard wizard, final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
        setSizeFull();

        GridLayout grid = new GridLayout( 1, 5 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label confirmationLbl = new Label( "<strong>Please verify the installation settings "
                + "(you may change them by clicking on Back button)</strong><br/>" );
        confirmationLbl.setContentMode( ContentMode.HTML );

        ConfigView cfgView = new ConfigView( "Installation configuration" );
        cfgView.addStringCfg( "Cluster Name", wizard.getConfig().getClusterName() );
        HadoopClusterConfig hcc =
                wizard.getHadoopManager().getCluster( wizard.getConfig().getHadoopClusterName() );

        ContainerHost host = environmentManager.getEnvironmentByUUID(hcc.getEnvironmentId()).getContainerHostById(wizard.getConfig().getServer());
        cfgView.addStringCfg( "Server", host.getHostname() + "\n" );
        if ( wizard.getConfig().getClients() != null )
        {
            Set<UUID> nodes = new HashSet<>(wizard.getConfig().getClients());
            Set<ContainerHost> hosts = environmentManager.getEnvironmentByUUID(hcc.getEnvironmentId()).getContainerHostsByIds(nodes);

            for ( ContainerHost containerHost : hosts)
            {
                cfgView.addStringCfg( "Clients", containerHost.getHostname() + "\n" );
            }
        }

        Button install = new Button( "Install" );
        install.setId( "OozieVerificationInstall" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackID = wizard.getOozieManager().installCluster( wizard.getConfig() );
                final ProgressWindow window = new ProgressWindow( wizard.getExecutor(), wizard.getTracker(), trackID,
                        OozieClusterConfig.PRODUCT_KEY );
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
        back.setId( "OozieVerificationBack" );
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
