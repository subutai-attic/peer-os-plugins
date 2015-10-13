package io.subutai.plugin.spark.ui.wizard;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class ConfigurationStep extends Panel
{
    private final Hadoop hadoop;
    private final EnvironmentManager environmentManager;
    private Environment hadoopEnvironment;
    private Wizard wizard;


    public ConfigurationStep( final Hadoop hadoop, final EnvironmentManager environmentManager, final Wizard wizard )
    {

        this.hadoop = hadoop;
        this.environmentManager = environmentManager;
        this.wizard = wizard;
        setSizeFull();

        GridLayout content = new GridLayout( 1, 4 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        TextField nameTxt = new TextField( "Cluster name" );
        nameTxt.setId( "sparkClusterName" );
        nameTxt.setInputPrompt( "Cluster name" );
        nameTxt.setRequired( true );
        nameTxt.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent e )
            {
                wizard.getConfig().setClusterName( e.getProperty().getValue().toString().trim() );
            }
        } );
        nameTxt.setValue( wizard.getConfig().getClusterName() );

        Button next = new Button( "Next" );
        next.setId( "sparkNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                nextClickHandler( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "sparkBack" );
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

        content.addComponent( nameTxt );

        addControls( content, wizard.getConfig() );

        content.addComponent( buttons );

        setContent( layout );
    }


    private void addControls( ComponentContainer parent, final SparkClusterConfig config )
    {
        final ComboBox hadoopClustersCombo = new ComboBox( "Hadoop cluster" );
        final ComboBox masterNodeCombo = new ComboBox( "Master node" );
        final TwinColSelect slaveNodesSelect =
                new TwinColSelect( "Slave nodes", Lists.<EnvironmentContainerHost>newArrayList() );

        hadoopClustersCombo.setId( "sparkHadoopCluster" );
        masterNodeCombo.setId( "sparkMasterNode" );
        slaveNodesSelect.setId( "sparkSlaveNodes" );

        masterNodeCombo.setImmediate( true );
        masterNodeCombo.setTextInputAllowed( false );
        masterNodeCombo.setRequired( true );
        masterNodeCombo.setNullSelectionAllowed( false );

        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        hadoopClustersCombo.setNullSelectionAllowed( false );

        slaveNodesSelect.setItemCaptionPropertyId( "hostname" );
        slaveNodesSelect.setRows( 7 );
        slaveNodesSelect.setMultiSelect( true );
        slaveNodesSelect.setImmediate( true );
        slaveNodesSelect.setLeftColumnCaption( "Available Nodes" );
        slaveNodesSelect.setRightColumnCaption( "Selected Nodes" );
        slaveNodesSelect.setWidth( 100, Unit.PERCENTAGE );
        slaveNodesSelect.setRequired( true );

        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        if ( !clusters.isEmpty() )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClustersCombo.addItem( hadoopClusterInfo );
                hadoopClustersCombo.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            if ( !clusters.isEmpty() )
            {
                hadoopClustersCombo.setValue( clusters.iterator().next() );
            }
        }
        else
        {
            HadoopClusterConfig info = hadoop.getCluster( config.getHadoopClusterName() );
            if ( info != null )
            //restore cluster
            {
                hadoopClustersCombo.setValue( info );
            }
            else if ( !clusters.isEmpty() )
            {
                hadoopClustersCombo.setValue( clusters.iterator().next() );
            }
        }


        if ( hadoopClustersCombo.getValue() != null )
        {
            HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) hadoopClustersCombo.getValue();
            config.setHadoopClusterName( hadoopInfo.getClusterName() );
            try
            {
                hadoopEnvironment = environmentManager.loadEnvironment( hadoopInfo.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                Notification.show( String.format( "Failed to obtain Hadoop environment: %s", e ) );
            }
            Set<EnvironmentContainerHost> hadoopNodes = Sets.newHashSet();
            try
            {
                for ( String nodeId : filterNodes( hadoopInfo.getAllNodes() ) )
                {
                    hadoopNodes.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                }

                slaveNodesSelect.setContainerDataSource(
                        new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopNodes ) );
                for ( EnvironmentContainerHost hadoopNode : hadoopNodes )
                {
                    masterNodeCombo.addItem( hadoopNode );
                    masterNodeCombo.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                Notification.show( String.format( "Failed to obtain Hadoop environment containers: %s", e ) );
            }
        }

        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    try
                    {
                        hadoopEnvironment = environmentManager.loadEnvironment( hadoopInfo.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        Notification.show( String.format( "Failed to obtain Hadoop environment: %s", e ) );
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
                        Notification.show( String.format( "Failed to obtain Hadoop environment containers: %s", e ) );
                        return;
                    }
                    slaveNodesSelect.setValue( null );
                    slaveNodesSelect.setContainerDataSource(
                            new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopNodes ) );
                    masterNodeCombo.setValue( null );
                    masterNodeCombo.removeAllItems();
                    for ( EnvironmentContainerHost hadoopNode : hadoopNodes )
                    {
                        masterNodeCombo.addItem( hadoopNode );
                        masterNodeCombo.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                    }
                    config.setHadoopClusterName( hadoopInfo.getClusterName() );
                    config.getSlaveIds().clear();
                    config.setMasterNodeId( null );
                }
            }
        } );

        masterNodeCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    EnvironmentContainerHost master = ( EnvironmentContainerHost ) event.getProperty().getValue();
                    config.setMasterNodeId( master.getId() );

                    //fill slave nodes without newly selected master node
                    config.getSlaveIds().remove( master.getId() );

                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) hadoopClustersCombo.getValue();
                    try
                    {
                        hadoopEnvironment = environmentManager.loadEnvironment( hadoopInfo.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        Notification.show( String.format( "Failed to obtain Hadoop environment: %s", e ) );
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
                        Notification.show( String.format( "Failed to obtain Hadoop environment containers: %s", e ) );
                        return;
                    }
                    for ( Iterator<EnvironmentContainerHost> iterator = hadoopNodes.iterator(); iterator.hasNext(); )
                    {
                        final EnvironmentContainerHost haddopNode = iterator.next();

                        if ( haddopNode.getId().equals( master.getId() ) )
                        {
                            iterator.remove();
                            break;
                        }
                    }
                    slaveNodesSelect.getContainerDataSource().removeAllItems();
                    for ( EnvironmentContainerHost hadoopNode : hadoopNodes )
                    {
                        slaveNodesSelect.getContainerDataSource().addItem( hadoopNode );
                    }

                    Collection ls = slaveNodesSelect.getListeners( Property.ValueChangeListener.class );
                    Property.ValueChangeListener h =
                            ls.isEmpty() ? null : ( Property.ValueChangeListener ) ls.iterator().next();
                    if ( h != null )
                    {
                        slaveNodesSelect.removeValueChangeListener( h );
                    }

                    try
                    {
                        List<EnvironmentContainerHost> containerHostList = new ArrayList<>();
                        for ( String id : config.getSlaveIds() )
                        {
                            containerHostList.add( hadoopEnvironment.getContainerHostById( id ) );
                        }
                        slaveNodesSelect.setValue( containerHostList );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        Notification.show( String
                                .format( "Failed to obtain Hadoop environment containers selected as Spark slaves: %s",
                                        e ) );
                        return;
                    }
                    if ( h != null )
                    {
                        slaveNodesSelect.addValueChangeListener( h );
                    }
                }
            }
        } );

        if ( config.getMasterNodeId() != null )
        {
            try
            {
                masterNodeCombo.setValue( hadoopEnvironment.getContainerHostById( config.getMasterNodeId() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                Notification.show( String
                        .format( "Failed to obtain Hadoop environment container selected as Spark master: %s", e ) );
                return;
            }
        }

        if ( !CollectionUtil.isCollectionEmpty( config.getSlaveIds() ) )
        {
            try
            {
                slaveNodesSelect.setValue( hadoopEnvironment.getContainerHostsByIds( config.getSlaveIds() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                Notification.show( String
                        .format( "Failed to obtain Hadoop environment containers selected as Spark slaves: %s", e ) );
                return;
            }
        }

        slaveNodesSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null && event.getProperty().getValue() instanceof Set )
                {
                    Set nodes = ( Set ) event.getProperty().getValue();
                    Set<String> slaveIds = Sets.newHashSet();
                    for ( Object node : nodes )
                    {
                        if ( node instanceof EnvironmentContainerHost )
                        {
                            EnvironmentContainerHost containerHost = ( EnvironmentContainerHost ) node;
                            slaveIds.add( containerHost.getId() );
                        }
                    }
                    config.getSlaveIds().clear();
                    config.getSlaveIds().addAll( slaveIds );
                }
            }
        } );

        parent.addComponent( hadoopClustersCombo );
        parent.addComponent( masterNodeCombo );
        parent.addComponent( slaveNodesSelect );
    }


    //exclude hadoop nodes that are already in another hipi cluster
    private List<String> filterNodes( List<String> hadoopNodes )
    {
        List<String> sparkNodes = new ArrayList<>();
        List<String> filteredNodes = new ArrayList<>();
        for ( SparkClusterConfig hipiConfig : wizard.getSparkManager().getClusters() )
        {
            sparkNodes.addAll( hipiConfig.getAllNodesIds() );
        }
        for ( String node : hadoopNodes )
        {
            if ( !sparkNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }


    private void nextClickHandler( Wizard wizard )
    {
        SparkClusterConfig config = wizard.getConfig();
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            show( "Enter cluster name" );
        }
        else if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            show( "Please, select Hadoop cluster" );
        }
        else if ( config.getMasterNodeId() == null )
        {
            show( "Please, select master node" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getSlaveIds() ) )
        {
            show( "Please, select slave node(s)" );
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
