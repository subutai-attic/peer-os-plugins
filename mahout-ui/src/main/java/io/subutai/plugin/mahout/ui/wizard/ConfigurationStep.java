/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.ui.wizard;


import java.util.ArrayList;
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
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.ComponentContainer;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;


public class ConfigurationStep extends Panel
{
    private final static Logger LOGGER = LoggerFactory.getLogger( ConfigurationStep.class );

    private final Hadoop hadoop;
    private final EnvironmentManager environmentManager;
    final Wizard wizard;
    private Environment hadoopEnvironment;


    public ConfigurationStep( final Hadoop hadoop, final Wizard wizard, final EnvironmentManager environmentManager )
    {

        this.hadoop = hadoop;
        this.environmentManager = environmentManager;
        this.wizard = wizard;

        setSizeFull();

        GridLayout content = new GridLayout( 1, 2 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        VerticalLayout layout = new VerticalLayout();
        layout.setSpacing( true );
        layout.addComponent( new Label( "Please, specify installation settings" ) );
        layout.addComponent( content );

        TextField txtClusterName = new TextField( "Mahout installation name: " );
        txtClusterName.setId( "MahoutInstallationName" );
        txtClusterName.setRequired( true );
        txtClusterName.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String v = event.getProperty().getValue().toString().trim();
                wizard.getConfig().setClusterName( v );
            }
        } );
        txtClusterName.setValue( wizard.getConfig().getClusterName() );

        content.addComponent( txtClusterName );


        addOverHadoopControls( content, wizard.getConfig() );


        // --- buttons ---
        Button next = new Button( "Next" );
        next.setId( "MahoutConfNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                nextButtonClickHandler( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "MahoutConfBack" );
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
        buttons.addComponent( next );

        content.addComponent( buttons );

        setContent( layout );
    }


    private void addOverHadoopControls( ComponentContainer parent, final MahoutClusterConfig config )
    {
        final TwinColSelect select = new TwinColSelect( "Nodes", new ArrayList<ContainerHost>() );
        select.setId( "MahoutConfSlaveNodes" );

        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "MahoutConfHadoopCluster" );
        hadoopClusters.setImmediate( true );
        hadoopClusters.setTextInputAllowed( false );
        hadoopClusters.setRequired( true );
        hadoopClusters.setNullSelectionAllowed( false );
        hadoopClusters.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    config.setHadoopClusterName( hadoopInfo.getClusterName() );
                    try
                    {
                        hadoopEnvironment = environmentManager.findEnvironment( hadoopInfo.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Error getting environment by id: " + hadoopInfo.getEnvironmentId().toString(),
                                e );
                        return;
                    }

                    Set<ContainerHost> hadoopNodes = Sets.newHashSet();
                    try
                    {
                        for ( UUID nodeId : filterNodes( hadoopInfo.getAllNodes() ) )
                        {
                            hadoopNodes.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                        }
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        show( String.format( "Error accessing environment: %s", e ) );
                        return;
                    }
                    select.setValue( null );
                    select.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, hadoopNodes ) );
                }
            }
        } );

        Hadoop hadoopManager = hadoop;
        List<HadoopClusterConfig> clusters = hadoopManager.getClusters();
        if ( !CollectionUtil.isCollectionEmpty( clusters ) )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClusters.addItem( hadoopClusterInfo );
                hadoopClusters.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        String hcn = config.getHadoopClusterName();
        if ( hcn != null )
        {
            HadoopClusterConfig info = hadoopManager.getCluster( hcn );
            if ( info != null )
            {
                hadoopClusters.setValue( info );
            }
        }
        else if ( !CollectionUtil.isCollectionEmpty( clusters ) )
        {
            hadoopClusters.setValue( clusters.iterator().next() );
        }

        select.setItemCaptionPropertyId( "hostname" );
        select.setRows( 7 );
        select.setMultiSelect( true );
        select.setImmediate( true );
        select.setLeftColumnCaption( "Available Nodes" );
        select.setRightColumnCaption( "Selected Nodes" );
        select.setWidth( 100, Unit.PERCENTAGE );
        select.setRequired( true );
        if ( !CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            select.setValue( config.getNodes() );
        }
        select.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<UUID> nodes = new HashSet<UUID>();
                    Set<ContainerHost> nodeList = ( Set<ContainerHost> ) event.getProperty().getValue();
                    for ( ContainerHost host : nodeList )
                    {
                        nodes.add( host.getId() );
                    }
                    config.getNodes().clear();
                    config.getNodes().addAll( nodes );
                }
            }
        } );

        parent.addComponent( hadoopClusters );
        parent.addComponent( select );
    }


    //exclude hadoop nodes that are already in another Mahout cluster
    private List<UUID> filterNodes( List<UUID> hadoopNodes )
    {
        List<UUID> mahoutNodes = new ArrayList<>();
        List<UUID> filteredNodes = new ArrayList<>();
        for ( MahoutClusterConfig mahoutConfig : wizard.getMahoutManager().getClusters() )
        {
            mahoutNodes.addAll( mahoutConfig.getNodes() );
        }
        for ( UUID node : hadoopNodes )
        {
            if ( !mahoutNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }


    private void nextButtonClickHandler( Wizard wizard )
    {
        MahoutClusterConfig config = wizard.getConfig();

        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            show( "Enter installation name" );
        }
        else if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            show( "Select Hadoop cluster" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            show( "Select target nodes" );
        }
        else
        {
            wizard.next();
        }
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
