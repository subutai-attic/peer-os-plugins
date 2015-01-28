package org.safehaus.subutai.plugin.sqoop.ui.wizard;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

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


public class NodeSelectionStep extends VerticalLayout
{
    private final Hadoop hadoop;
    private final EnvironmentManager environmentManager;
    private final Wizard wizard;

    public NodeSelectionStep( final Hadoop hadoop, EnvironmentManager environmentManager, final Wizard wizard )
    {

        this.hadoop = hadoop;
        this.wizard = wizard;
        this.environmentManager = environmentManager;

        setSizeFull();

        GridLayout content = new GridLayout( 1, 2 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        VerticalLayout layout = new VerticalLayout();
        layout.setSpacing( true );
        layout.addComponent( new Label( "Please, specify installation settings" ) );
        layout.addComponent( content );

        TextField txtClusterName = new TextField( "Sqoop installation name: " );
        txtClusterName.setId( "sqoopInstallationName" );
        txtClusterName.setRequired( true );
        txtClusterName.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Object v = event.getProperty().getValue();
                if ( v != null )
                {
                    wizard.getConfig().setClusterName( v.toString().trim() );
                }
            }
        } );
        txtClusterName.setValue( wizard.getConfig().getClusterName() );

        content.addComponent( txtClusterName );

        addOverHadoopControls( content, wizard.getConfig() );

        // --- buttons ---
        Button next = new Button( "Next" );
        next.setId( "sqoopInstNext" );
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
        back.setId( "sqoopInstBack" );
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


    private void addOverHadoopControls( ComponentContainer parent, final SqoopConfig config )
    {
        final TwinColSelect select = new TwinColSelect( "Nodes", new ArrayList<ContainerHost>() );
        select.setId( "sqoopSlaveNodes" );

        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "sqoopHadoopCluster" );
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
                    Environment env = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
                    Set<ContainerHost> allNodes = env.getContainerHostsByIds(
                            new HashSet<>( hadoopInfo.getAllNodes() ) );
                    select.setValue( null );
                    select.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, allNodes ) );
                    wizard.getConfig().setHadoopClusterName( hadoopInfo.getClusterName() );
                    wizard.getConfig().setEnvironmentId( hadoopInfo.getEnvironmentId() );
                }
            }
        } );

        List<HadoopClusterConfig> clusters = hadoop.getClusters();
        if ( clusters != null )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClusters.addItem( hadoopClusterInfo );
                hadoopClusters.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        String hcn = config.getHadoopClusterName();
        if ( hcn != null && !hcn.isEmpty() )
        {
            HadoopClusterConfig info = hadoop.getCluster( hcn );
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
            HadoopClusterConfig hadoopConfig = hadoop.getCluster( config.getHadoopClusterName() );
            if ( hadoopConfig != null )
            {
                Environment env = environmentManager.getEnvironmentByUUID( hadoopConfig.getEnvironmentId() );
                if ( env != null )
                {
                    Set<ContainerHost> hosts = env.getContainerHostsByIds( config.getNodes() );
                    select.setValue( hosts );
                }
            }
        }
        select.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                config.getNodes().clear();
                if ( event.getProperty().getValue() != null )
                {
                    Collection selected = ( Collection ) event.getProperty().getValue();
                    for ( Object obj : selected )
                    {
                        if ( obj instanceof ContainerHost )
                        {
                            config.getNodes().add( ( ( ContainerHost ) obj ).getId() );
                        }
                    }
                }
            }
        } );

        parent.addComponent( hadoopClusters );
        parent.addComponent( select );
    }


    private void nextButtonClickHandler( Wizard wizard )
    {
        SqoopConfig config = wizard.getConfig();
        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            show( "Enter installation name" );
            return;
        }

        String name = config.getHadoopClusterName();
        if ( name == null || name.isEmpty() )
        {
            show( "Select Hadoop cluster" );
        }
        else if ( config.getNodes() == null || config.getNodes().isEmpty() )
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
