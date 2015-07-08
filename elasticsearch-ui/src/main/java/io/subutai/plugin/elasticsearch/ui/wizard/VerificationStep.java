package io.subutai.plugin.elasticsearch.ui.wizard;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.ui.ConfigView;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.server.ui.component.ProgressWindow;

import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


public class VerificationStep extends VerticalLayout
{

    public VerificationStep( final Elasticsearch elasticsearch, final ExecutorService executorService,
                             final Tracker tracker, final EnvironmentWizard environmentWizard )
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

        String selectedNodes = "";
        Environment environment = null;
        try
        {
            environment = environmentWizard.getEnvironmentManager().findEnvironment(
                    environmentWizard.getConfig().getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        for ( UUID uuid : environmentWizard.getConfig().getNodes() )
        {
            if ( environment != null )
            {
                try
                {
                    selectedNodes += environment.getContainerHostById( uuid ).getHostname() + ",";
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
        }

        cfgView.addStringCfg( "Nodes to be configured", selectedNodes.substring( 0, ( selectedNodes.length() - 1 ) ) );
        cfgView.addStringCfg( "Environment UUID", environmentWizard.getConfig().getEnvironmentId() + "" );

        Button install = new Button( "Configure" );
        install.addStyleName( "default" );
        install.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                UUID trackID = elasticsearch.installCluster( environmentWizard.getConfig() );
                ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                        ElasticsearchClusterConfiguration.PRODUCT_KEY );
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
