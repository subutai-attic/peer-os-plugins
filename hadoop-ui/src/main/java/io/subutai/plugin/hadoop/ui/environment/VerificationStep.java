/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.hadoop.ui.environment;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.server.ui.component.ProgressWindow;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( VerificationStep.class );
    public VerificationStep( final Hadoop hadoop, final ExecutorService executorService, final Tracker tracker,
                             final EnvironmentWizard wizard )
    {


        try
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
            cfgView.addStringCfg( "Cluster Name", wizard.getHadoopClusterConfig().getClusterName() );
            cfgView.addStringCfg( "Domain Name", wizard.getHadoopClusterConfig().getDomainName() );
            cfgView.addStringCfg( "Number of slave nodes",
                    wizard.getHadoopClusterConfig().getAllSlaveNodes().size() + "" );
            cfgView.addStringCfg( "Replication factor", wizard.getHadoopClusterConfig().getReplicationFactor() + "" );

            final Environment environment = wizard.getEnvironmentManager().findEnvironment(
                    wizard.getHadoopClusterConfig().getEnvironmentId() );
            cfgView.addStringCfg( "NameNode",
                    environment.getContainerHostById( wizard.getHadoopClusterConfig().getNameNode() ).getHostname()
                            + "" );
            cfgView.addStringCfg( "JobTracker",
                    environment.getContainerHostById( wizard.getHadoopClusterConfig().getJobTracker() ).getHostname()
                            + "" );
            cfgView.addStringCfg( "SecondaryNameNode",
                    environment.getContainerHostById( wizard.getHadoopClusterConfig().getSecondaryNameNode() )
                               .getHostname() + "" );
            String selectedNodes = "";
            for ( UUID uuid : wizard.getHadoopClusterConfig().getAllSlaveNodes() )
            {
                selectedNodes += environment.getContainerHostById( uuid ).getHostname() + ",";
            }
            cfgView.addStringCfg( "Slave Nodes (DataNode & TaskTracker)",
                    selectedNodes.substring( 0, ( selectedNodes.length() - 1 ) ) );

            Button install = new Button( "Configure" );
            install.setId( "HadoopBtnInstall" );
            install.addStyleName( "default" );
            install.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    UUID trackID = hadoop.installCluster( wizard.getHadoopClusterConfig() );
                    ProgressWindow window =
                            new ProgressWindow( executorService, tracker, trackID, HadoopClusterConfig.PRODUCT_KEY );
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
            back.setId( "HadoopVerBack" );
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

            addComponent( grid );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            LOGGER.error( "Environment error", e );
        }
    }
}
