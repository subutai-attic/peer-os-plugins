package io.subutai.plugin.lucene.ui.wizard;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;


public class ConfigurationStep extends Panel
{
    private final Hadoop hadoop;
    private final EnvironmentManager environmentManager;
    final Wizard wizard;


    public ConfigurationStep( final Hadoop hadoop, final Wizard wizard, final EnvironmentManager environmentManager )
    {
        this.hadoop = hadoop;
        this.environmentManager = environmentManager;
        this.wizard = wizard;

        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );


        addOverHadoopControls( content, wizard.getConfig() );


        // --------------------------------------------------
        // Buttons

        Button next = new Button( "Next" );
        next.setId( "LuceneConfNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                nextButtonClickHandler( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "LuceneConfBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                wizard.back();
            }
        } );

        VerticalLayout layout = new VerticalLayout();
        layout.setSpacing( true );
        layout.addComponent( new Label( "Please, specify installation settings" ) );
        layout.addComponent( content );

        HorizontalLayout buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( next );

        content.addComponent( buttons );

        setContent( layout );
    }


    private void addOverHadoopControls( ComponentContainer parent, final LuceneConfig config )
    {
        TextField nameTxt = new TextField( "Cluster name" );
        nameTxt.setId( "luceneClusterName" );
        nameTxt.setInputPrompt( "Cluster name" );
        nameTxt.setRequired( true );
        nameTxt.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent e )
            {
                config.setClusterName( e.getProperty().getValue().toString().trim() );
            }
        } );
        nameTxt.setValue( wizard.getConfig().getClusterName() );

        final TwinColSelect select = new TwinColSelect( "Nodes", new ArrayList<EnvironmentContainerHost>() );

        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "LuceneHadoopClusters" );
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
                    Set<EnvironmentContainerHost> hadoopNodes = Sets.newHashSet();
                    try
                    {
                        Environment environment = environmentManager.loadEnvironment( hadoopInfo.getEnvironmentId() );
                        for ( String nodeId : filterNodes( hadoopInfo.getAllNodes() ) )
                        {
                            hadoopNodes.add( environment.getContainerHostById( nodeId ) );
                        }
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        show( String.format( "Failed obtaining environment containers: %s", e ) );
                        return;
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        show( String.format( "Environment not found: %s", e ) );
                        return;
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
        else if ( clusters != null && !clusters.isEmpty() )
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

        if ( config.getNodes() != null && !config.getNodes().isEmpty() )
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
        parent.addComponent( nameTxt );
        parent.addComponent( hadoopClusters );
        parent.addComponent( select );
    }


    //exclude hadoop nodes that are already in another lucene cluster
    private List<String> filterNodes( List<String> hadoopNodes )
    {
        List<String> luceneNodes = new ArrayList<>();
        List<String> filteredNodes = new ArrayList<>();
        for ( LuceneConfig flumeConfig : wizard.getLucene().getClusters() )
        {
            luceneNodes.addAll( flumeConfig.getNodes() );
        }
        for ( String node : hadoopNodes )
        {
            if ( !luceneNodes.contains( node ) )
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


    private void nextButtonClickHandler( Wizard wizard )
    {
        LuceneConfig config = wizard.getConfig();
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
}
