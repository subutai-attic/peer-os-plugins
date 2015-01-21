package org.safehaus.subutai.plugin.oozie.ui.wizard;


import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.*;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.Oozie;

import java.util.*;


public class StepSetConfig extends Panel
{
    public EnvironmentManager environmentManager;

    public StepSetConfig( final Oozie oozie, final Hadoop hadoop, final Wizard wizard , final EnvironmentManager environmentManager)
    {
        this.environmentManager = environmentManager;

        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSizeFull();
        verticalLayout.setHeight( 100, Unit.PERCENTAGE );
        verticalLayout.setMargin( true );

        GridLayout grid = new GridLayout( 10, 10 );
        grid.setSpacing( true );
        grid.setSizeFull();

        Panel panel = new Panel();
        Label menu = new Label( "Oozie Installation Wizard" );

        menu.setContentMode( ContentMode.HTML );
        panel.setContent( menu );
        grid.addComponent( menu, 0, 0, 2, 1 );
        //		grid.setComponentAlignment(panel, Alignment.TOP_CENTER);

        VerticalLayout vl = new VerticalLayout();
        vl.setSizeFull();
        vl.setSpacing( true );

        Label configServersLabel = new Label( "<strong>Oozie Server</strong>" );
        configServersLabel.setContentMode( ContentMode.HTML );
        vl.addComponent( configServersLabel );

        final Label server = new Label( "Server" );
        vl.addComponent( server );

        final ComboBox cbServers = new ComboBox();
        cbServers.setId( "OozieConfServersCombo" );
        cbServers.setImmediate( true );
        cbServers.setTextInputAllowed( false );
        cbServers.setRequired( true );
        cbServers.setNullSelectionAllowed( false );

        final HadoopClusterConfig hcc =
                wizard.getHadoopManager().getCluster( wizard.getConfig().getHadoopClusterName() );

        List<UUID> allHadoopNodes = hcc.getAllNodes();
        Set<UUID> allHadoopNodeSet = new HashSet<>();
        allHadoopNodeSet.addAll( allHadoopNodes );

        Set<UUID> nodes = new HashSet<>(hcc.getAllNodes());
        final Set<ContainerHost> hosts = environmentManager.getEnvironmentByUUID(hcc.getEnvironmentId()).getContainerHostsByIds(nodes);

        for (ContainerHost host : hosts)
        {
            cbServers.addItem( host );
            cbServers.setItemCaption( host, host.getHostname() );
        }

//        for ( UUID agent : hcc.getAllNodes() )
//        {
//            cbServers.addItem( agent );
//            cbServers.setItemCaption( agent, agent.toString() );
//        }



        vl.addComponent( cbServers );

        if ( wizard.getConfig().getServer() != null )
        {
            cbServers.setValue( wizard.getConfig().getServer() );
        }

        final TwinColSelect selectClients = new TwinColSelect( "", new ArrayList<String>() );
        selectClients.setId( "OozieConfClientNodes" );
        selectClients.setItemCaptionPropertyId( "hostname" );
        selectClients.setRows( 7 );
        selectClients.setNullSelectionAllowed( true );
        selectClients.setMultiSelect( true );
        selectClients.setImmediate( true );
        selectClients.setLeftColumnCaption( "Available nodes" );
        selectClients.setRightColumnCaption( "Client nodes" );
        selectClients.setWidth( 100, Unit.PERCENTAGE );
        selectClients.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, hosts ) );

        if ( !CollectionUtil.isCollectionEmpty( wizard.getConfig().getClients() ) )
        {
            selectClients.setValue( wizard.getConfig().getClients() );
        }

        vl.addComponent( selectClients );

        grid.addComponent( vl, 3, 0, 9, 9 );
        grid.setComponentAlignment( vl, Alignment.TOP_CENTER );

        Button next = new Button( "Next" );
        next.setId( "OozieConfWizardNext" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                ContainerHost containerID = (ContainerHost) cbServers.getValue();
                wizard.getConfig().setServer(containerID.getId());
//                Set<UUID> clientNodes = new HashSet<>();
                if ( selectClients.getValue() != null )
                {

                    Set<ContainerHost> containerHosts =
                            new HashSet<>( ( Collection<ContainerHost> ) selectClients.getValue() );
                    Set<UUID> containerIDs = new HashSet<>();
                    for ( ContainerHost containerHost : containerHosts )
                    {
                        containerIDs.add( containerHost.getId() );
                    }
                    wizard.getConfig().setClients(containerIDs);

//                    for ( UUID node : ( Set<UUID> ) selectClients.getValue() )
//                    {
//                        clientNodes.add( node );
//                    }
//                    wizard.getConfig().setClients( clientNodes );
                }

                if ( wizard.getConfig().getServer() == null )
                {
                    show( "Please select node for Oozie server" );
                }
                else if ( wizard.getConfig().getClients() != null && wizard.getConfig().getClients()
                                                                           .contains( wizard.getConfig().getServer() ) )
                {
                    show( "Oozie server and client can not be installed on the same host" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "OozieConfWizardBack" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                wizard.back();
            }
        } );

        cbServers.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                ContainerHost selectedServerNode = ( ContainerHost ) event.getProperty().getValue();
                Set<ContainerHost> hadoopNodes = hosts;
                List<ContainerHost> availableOozieClientNodes = new ArrayList<>();
                availableOozieClientNodes.addAll( hadoopNodes );
                availableOozieClientNodes.remove( selectedServerNode );
                selectClients
                        .setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, availableOozieClientNodes ) );
                selectClients.markAsDirty();
            }
        } );


        verticalLayout.addComponent( grid );

        HorizontalLayout horizontalLayout = new HorizontalLayout();
        horizontalLayout.addComponent( back );
        horizontalLayout.addComponent( next );
        verticalLayout.addComponent( horizontalLayout );

        setContent( verticalLayout );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
