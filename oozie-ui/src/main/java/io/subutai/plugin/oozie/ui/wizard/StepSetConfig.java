package io.subutai.plugin.oozie.ui.wizard;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;


public class StepSetConfig extends Panel
{
    private final static Logger LOGGER = LoggerFactory.getLogger( StepSetConfig.class );

    public EnvironmentManager environmentManager;
    private Set<ContainerHost> hosts = null;
    private Wizard wizard;
    private Environment hadoopEnvironment;


    public StepSetConfig( final Oozie oozie, final Hadoop hadoop, final Wizard wizard,
                          final EnvironmentManager environmentManager ) throws EnvironmentNotFoundException
    {
        this.environmentManager = environmentManager;
        this.wizard = wizard;

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

        hadoopEnvironment = environmentManager.findEnvironment( hcc.getEnvironmentId() );
        final Set<ContainerHost> hadoopNodes = Sets.newHashSet();
        for ( ContainerHost host : hadoopEnvironment.getContainerHosts() )
        {
            for ( UUID nodeId : filterNodes( hcc.getAllNodes() ) )
            {
                try
                {
                    hadoopNodes.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    show( String.format( "Error accessing environment: %s", e ) );
                    return;
                }
            }
            if ( filterNodes( hcc.getAllNodes() ).contains( host.getId() ) )
            {
                cbServers.addItem( host );
                cbServers.setItemCaption( host, host.getHostname() );
            }
        }
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
        selectClients.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, hadoopNodes ) );

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
                Set<ContainerHost> containerHosts =
                        new HashSet<>( ( Collection<ContainerHost> ) selectClients.getValue() );

                if ( cbServers.getValue() == null || containerHosts.isEmpty() )
                {
                    show( "Please specify nodes for installation" );
                }

                if ( cbServers.getValue() != null )
                {
                    ContainerHost containerID = ( ContainerHost ) cbServers.getValue();
                    wizard.getConfig().setServer( containerID.getId() );
                }

                Set<UUID> containerIDs = new HashSet<>();
                if ( !containerHosts.isEmpty() )
                {

                        for( ContainerHost host : containerHosts )
                        {
                            containerIDs.add( host.getId() );
                        }

                    wizard.getConfig().setClients( containerIDs );
                }

                if ( wizard.getConfig().getServer() == null )
                {
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
                //Set<ContainerHost> hadoopNodes = hadoopEnvironment.getContainerHosts();
                List<ContainerHost> availableOozieClientNodes = new ArrayList<>();
                availableOozieClientNodes.addAll( hadoopNodes );
                availableOozieClientNodes.remove( selectedServerNode );
                selectClients.setContainerDataSource(
                        new BeanItemContainer<>( ContainerHost.class, availableOozieClientNodes ) );
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


    //exclude hadoop nodes that are already in another oozie cluster
    private List<UUID> filterNodes( List<UUID> hadoopNodes )
    {
        List<UUID> oozieNodes = new ArrayList<>();
        List<UUID> filteredNodes = new ArrayList<>();
        for ( OozieClusterConfig oozieConfig : wizard.getOozieManager().getClusters() )
        {
            oozieNodes.addAll( oozieConfig.getAllNodes() );
        }
        for ( UUID node : hadoopNodes )
        {
            if ( !oozieNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
