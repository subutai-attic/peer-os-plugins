package io.subutai.plugin.flume.ui.wizard;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class ConfigurationStep extends VerticalLayout
{

    private final Hadoop hadoop;
    private final EnvironmentManager environmentManager;
    private Environment hadoopEnvironment;
    final Wizard wizard;
    private final static Logger LOGGER = LoggerFactory.getLogger( ConfigurationStep.class );


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

        TextField txtClusterName = new TextField( "Flume installation name: " );
        txtClusterName.setId( "FlumeClusterName" );
        txtClusterName.setRequired( true );
        txtClusterName.setValue( wizard.getConfig().getClusterName() );
        txtClusterName.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    String v = event.getProperty().getValue().toString().trim();
                    wizard.getConfig().setClusterName( v );
                }
            }
        } );

        content.addComponent( txtClusterName );


        addSettingsControls( content, wizard.getConfig() );


        // --- buttons ---
        Button next = new Button( "Next" );
        next.setId( "FluConfigNext" );
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
        back.setId( "FluConfigBack" );
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

        addComponent( layout );
    }


    private void addSettingsControls( ComponentContainer parent, final FlumeConfig config )
    {
        final TwinColSelect select = new TwinColSelect( "Nodes", new ArrayList<EnvironmentContainerHost>() );

        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "FluHadoopClustersCb" );
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
                        hadoopEnvironment = environmentManager.loadEnvironment( hadoopInfo.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Error getting environment by id: " + hadoopInfo.getEnvironmentId(), e );
                        return;
                    }
                    Set<EnvironmentContainerHost> hadoopNodes = Sets.newHashSet();
                    try
                    {
                        for ( String nodeId : filterNodes( hadoopInfo.getAllNodes() ) )
                        {
                            hadoopNodes.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                        }
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        LOGGER.error( "Container hosts not found", e );
                    }
                    select.setValue( null );
                    select.setContainerDataSource(
                            new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopNodes ) );
                }
            }
        } );

        Hadoop hadoopManager = hadoop;
        List<HadoopClusterConfig> clusters = hadoopManager.getClusters();
        if ( clusters != null )
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
                    Set<String> nodes = new HashSet<>();
                    Set<EnvironmentContainerHost> nodeList =
                            ( Set<EnvironmentContainerHost> ) event.getProperty().getValue();
                    for ( EnvironmentContainerHost host : nodeList )
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


    //exclude hadoop nodes that are already in another flume cluster
    private List<String> filterNodes( List<String> hadoopNodes )
    {
        List<String> flumeNodes = new ArrayList<>();
        List<String> filteredNodes = new ArrayList<>();
        for ( FlumeConfig flumeConfig : wizard.getFlumeManager().getClusters() )
        {
            flumeNodes.addAll( flumeConfig.getNodes() );
        }
        for ( String node : hadoopNodes )
        {
            if ( !flumeNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }


    private void nextButtonClickHandler( Wizard wizard )
    {
        FlumeConfig config = wizard.getConfig();
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
