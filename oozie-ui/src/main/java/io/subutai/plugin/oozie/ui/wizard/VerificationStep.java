package io.subutai.plugin.oozie.ui.wizard;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.server.ui.component.ProgressWindow;


public class VerificationStep extends Panel
{
    private final static Logger LOGGER = LoggerFactory.getLogger( VerificationStep.class );


    public VerificationStep( final Wizard wizard, final EnvironmentManager environmentManager )
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
        cfgView.addStringCfg( "Cluster Name", wizard.getConfig().getClusterName() );
        HadoopClusterConfig hcc = wizard.getHadoopManager().getCluster( wizard.getConfig().getHadoopClusterName() );

        ContainerHost host;
        try
        {
            host = environmentManager.loadEnvironment( hcc.getEnvironmentId() )
                                     .getContainerHostById( wizard.getConfig().getServer() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Container host not found", e );
            return;
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment by id: " + hcc.getEnvironmentId(), e );
            return;
        }
        cfgView.addStringCfg( "Server", host.getHostname() + "\n" );
        if ( wizard.getConfig().getClients() != null )
        {
            Set<String> nodes = new HashSet<>( wizard.getConfig().getClients() );
            Set<ContainerHost> hosts = Sets.newHashSet();
            try
            {
                Environment environment = environmentManager.loadEnvironment( hcc.getEnvironmentId() );

                for ( String uuid : nodes )
                {
                    try
                    {
                        hosts.add( environment.getContainerHostById( uuid ) );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        LOGGER.error( "Container host not found" );
                    }
                }
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Error getting environment by id: " + hcc.getEnvironmentId(), e );
                return;
            }

            for ( ContainerHost containerHost : hosts )
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
