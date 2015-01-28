/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.ui.wizard;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Peer;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.env.api.Environment;
import org.safehaus.subutai.core.env.api.exception.EnvironmentNotFoundException;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.data.Container;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.DefaultFieldFactory;
import com.vaadin.ui.Field;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Table;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;


/**
 * @author dilshat
 */
public class EnvironmentPeerSelectionStep extends VerticalLayout
{
    Logger LOGGER = LoggerFactory.getLogger( EnvironmentPeerSelectionStep.class );

    private TwinColSelect configServerNodes;
    private TwinColSelect routeServerNodes;
    private TwinColSelect dataServerNodes;

    private List<Peer> peers = new ArrayList<>();
    private Wizard wizard;


    public EnvironmentPeerSelectionStep( final Wizard wizard )
    {
        this.wizard = wizard;
        this.peers.addAll( wizard.getPeerManager().getPeers() );
        editableHeights();
    }


    private ComboBox getEnvironmentList( final Wizard wizard )
    {
        List<MongoClusterConfig> clusterConfigs = wizard.getMongo().getClusters();

        final Set<UUID> mongoContainerHosts = new HashSet<>();
        for ( final MongoClusterConfig clusterConfig : clusterConfigs )
        {
            for ( final MongoNode mongoNode : clusterConfig.getAllNodes() )
            {
                mongoContainerHosts.add( mongoNode.getContainerHost().getId() );
            }
        }

        List<Environment> environments = new ArrayList<>( wizard.getEnvironmentManager().getEnvironments() );
        final BeanContainer<String, Environment> container = new BeanContainer<>( Environment.class );
        container.setBeanIdProperty( "name" );
        container.addAll( environments );

        ComboBox envList = new ComboBox( "Select environment" );
        envList.setId( "envList" );
        envList.setItemCaptionPropertyId( "name" );
        envList.setItemCaptionMode( AbstractSelect.ItemCaptionMode.PROPERTY );
        envList.setImmediate( true );
        envList.setNullSelectionAllowed( false );
        envList.setTextInputAllowed( false );
        envList.setNullSelectionAllowed( false );
        envList.setWidth( 150, Unit.PIXELS );
        envList.setContainerDataSource( container );
        envList.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                BeanItem<Environment> clusterConfig = container.getItem( valueChangeEvent.getProperty().getValue() );

                Environment environment = clusterConfig.getBean();
                wizard.getMongoClusterConfig().setEnvironmentId( environment.getId() );
                wizard.getMongoClusterConfig().setCfgSrvPort( 27019 );
                wizard.getMongoClusterConfig().setDataNodePort( 27017 );
                wizard.getMongoClusterConfig().setRouterPort( 27018 );
                wizard.getMongoClusterConfig().setDomainName( Common.DEFAULT_DOMAIN_NAME );
                wizard.getMongoClusterConfig().setReplicaSetName( "repl" );
                wizard.getMongoClusterConfig().setNumberOfConfigServers( 1 );
                wizard.getMongoClusterConfig().setNumberOfDataNodes( 1 );
                wizard.getMongoClusterConfig().setNumberOfRouters( 1 );


                fillConfigServers( configServerNodes, environment.getContainerHosts(), mongoContainerHosts );
                fillConfigServers( routeServerNodes, environment.getContainerHosts(), mongoContainerHosts );
                fillConfigServers( dataServerNodes, environment.getContainerHosts(), mongoContainerHosts );
            }
        } );

        return envList;
    }


    private void fillConfigServers( TwinColSelect twinColSelect, Set<ContainerHost> containerHosts,
                                    final Set<UUID> mongoContainerHosts )
    {
        BeanContainer<String, ContainerHost> beanContainer = new BeanContainer<>( ContainerHost.class );
        beanContainer.setBeanIdProperty( "hostname" );

        List<ContainerHost> environmentHosts = new ArrayList<>();
        for ( final ContainerHost containerHost : containerHosts )
        {
            if ( !mongoContainerHosts.contains( containerHost.getId() ) && containerHost.getTemplateName()
                                                                                        .equalsIgnoreCase(
                                                                                                MongoClusterConfig
                                                                                                        .PRODUCT_NAME
                                                                                                         ) )
            {
                environmentHosts.add( containerHost );
            }
        }

        beanContainer.addAll( environmentHosts );

        twinColSelect.setContainerDataSource( beanContainer );
    }


    private TwinColSelect createTwinColSelect( String caption, String leftColumnCaption, String rightColumnCaption,
                                               int rows )
    {
        TwinColSelect twinColSelect = new TwinColSelect( caption );

        twinColSelect.setNullSelectionAllowed( false );
        twinColSelect.setMultiSelect( true );
        twinColSelect.setImmediate( true );
        twinColSelect.setLeftColumnCaption( leftColumnCaption );
        twinColSelect.setRightColumnCaption( rightColumnCaption );
        twinColSelect.setRows( rows );
        twinColSelect.setRequired( true );
        return twinColSelect;
    }


    void editableHeights()
    {
        VerticalLayout layout = new VerticalLayout();
        // BEGIN-EXAMPLE: component.table.editable.editableheights
        // Table with some typical data types
        final Table table = new Table( "Containers associated with peers." );
        table.addContainerProperty( "Container", String.class, null );
        table.addContainerProperty( "Peer", String.class, null );
        table.setDescription( "Associate container with peer." );
        table.setHeight( "200px" );
        table.setImmediate( true );

        Set<String> containerHostNames = new HashSet<>( wizard.getDataServerNames() );
        containerHostNames.addAll( wizard.getConfigServerNames() );
        containerHostNames.addAll( wizard.getRouterServerNames() );


        // Insert the data
        for ( final String containerHostName : containerHostNames )
        {
            Object obj[] = { containerHostName, "" };
            table.addItem( obj, containerHostName );
        }
        table.setPageLength( table.size() );

        // Set a custom field factory that overrides the default factory
        table.setTableFieldFactory( new DefaultFieldFactory()
        {
            private static final long serialVersionUID = -3301080798105311480L;


            @Override
            public Field<?> createField( Container container, Object itemId, Object propertyId, Component uiContext )
            {
                if ( "Peer".equals( propertyId ) )
                {
                    return propertyModeExample();
                }

                return super.createField( container, itemId, propertyId, uiContext );
            }
        } );
        table.setEditable( true );
        // Allow switching to non-editable mode
        final CheckBox editable = new CheckBox( "Table is editable", true );
        editable.addValueChangeListener( new Property.ValueChangeListener()
        {
            private static final long serialVersionUID = 6291942958587745232L;


            public void valueChange( Property.ValueChangeEvent event )
            {
                table.setEditable( editable.getValue() );
            }
        } );
        editable.setImmediate( true );
        // END-EXAMPLE: component.table.editable.editableheights
        table.addStyleName( "editableexample" );
        layout.addComponent( editable );
        layout.addComponent( table );

        addComponent( layout );
    }


    private ComboBox propertyModeExample()
    {

        // Have a bean container to put the beans in
        BeanItemContainer<Peer> container = new BeanItemContainer<>( Peer.class, peers );

        // Create a selection component bound to the container
        final ComboBox select = new ComboBox( "Inner Planets", container );

        // Set the caption mode to read the caption directly
        // from the 'name' property of the bean
        select.setItemCaptionMode( AbstractSelect.ItemCaptionMode.PROPERTY );
        select.setItemCaptionPropertyId( "name" );
        select.setImmediate( true );
        select.setTextInputAllowed( false );
        select.setRequired( true );
        select.setNullSelectionAllowed( false );

        // Handle selects
        select.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                // Get the selected item
                Object itemId = valueChangeEvent.getProperty().getValue();
                BeanItem<?> item = ( BeanItem<?> ) select.getItem( itemId );

                // Get the actual bean and use the data
                Peer planet = ( Peer ) item.getBean();
                show( "Clicked planet #" + planet.getName() );
                try
                {
                    Environment environment = wizard.getEnvironmentManager().findEnvironment(
                            wizard.getMongoClusterConfig().getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
        } );
        select.setImmediate( true );
        select.setNullSelectionAllowed( false );

        return select;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
