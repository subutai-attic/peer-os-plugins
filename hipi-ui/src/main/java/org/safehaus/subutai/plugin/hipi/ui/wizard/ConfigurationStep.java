package org.safehaus.subutai.plugin.hipi.ui.wizard;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import com.google.common.base.Strings;
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
    private final Hadoop hadoop;


    public ConfigurationStep( final Hadoop hadoop, final Wizard wizard )
    {

        this.hadoop = hadoop;

        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );


        addSettingsControls( content, wizard.getConfig() );


        // --------------------------------------------------
        // Buttons

        Button next = new Button( "Next" );
        next.setId( "hipiNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                nextButtonClick( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "hipiConfBack" );
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


    private void addSettingsControls( ComponentContainer parent, final HipiConfig config )
    {
        TextField nameTxt = new TextField( "Cluster name" );
        nameTxt.setId( "hipiClusterName" );
        nameTxt.setRequired( true );
        nameTxt.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent e )
            {
                config.setClusterName( e.getProperty().getValue().toString().trim() );
            }
        } );
        nameTxt.setValue( config.getClusterName() );

        final TwinColSelect select = new TwinColSelect( "Nodes", new ArrayList<UUID>() );

        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "hipiHadoopCluster" );
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
                    select.setValue( null );
                    select.setContainerDataSource( new BeanItemContainer<>( UUID.class, hadoopInfo.getAllNodes() ) );
                    config.setHadoopClusterName( hadoopInfo.getClusterName() );
                    config.getNodes().clear();
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
        select.setId( "hipiTwinCol" );
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
                    Collection agentList = ( Collection ) event.getProperty().getValue();
                    config.getNodes().clear();
                    config.getNodes().addAll( agentList );
                }
            }
        } );

        parent.addComponent( hadoopClusters );
        parent.addComponent( select );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }


    private void nextButtonClick( Wizard wizard )
    {
        HipiConfig config = wizard.getConfig();
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
