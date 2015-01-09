/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.ui.wizard;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.util.StringUtil;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoConfigNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoRouterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;


/**
 * @author dilshat
 */
public class EnvironmentConfigurationStep extends VerticalLayout
{
    Logger LOGGER = LoggerFactory.getLogger( EnvironmentConfigurationStep.class );

    private TwinColSelect configServerNodes;
    private TwinColSelect routeServerNodes;
    private TwinColSelect dataServerNodes;


    public EnvironmentConfigurationStep( final Wizard wizard )
    {

        setSizeFull();

        GridLayout content = new GridLayout( 3, 7 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        final ComboBox envList = getEnvironmentList( wizard );

        final TextField clusterNameTxtFld = new TextField( "Enter cluster name" );
        clusterNameTxtFld.setInputPrompt( "Cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setMaxLength( 20 );
        clusterNameTxtFld.setValue( wizard.getMongoClusterConfig().getClusterName() );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getMongoClusterConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        //configuration servers number
        configServerNodes = createTwinColSelect( "Select configuration node servers", "Available server nodes",
                "Selected server nodes", 0 );
        configServerNodes.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                if ( !valueChangeEvent.getProperty().getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            valueChangeEvent.getProperty().getValue().toString().replace( "[", "" ).replace( "]", "" )
                                            .split( "," );
                    BeanContainer<String, MongoConfigNode> container =
                            ( BeanContainer<String, MongoConfigNode> ) configServerNodes.getContainerDataSource();
                    Set<MongoConfigNode> mongoDataNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<MongoConfigNode> mongoBean = container.getItem( node.trim() );
                        MongoConfigNode dataNode = mongoBean.getBean();
                        mongoDataNodes.add( dataNode );
                    }
                    wizard.getMongoClusterConfig().setConfigServers( mongoDataNodes );
                }
            }
        } );

        //routers number
        routeServerNodes =
                createTwinColSelect( "Select route nodes.", "Available route nodes", "Selected route nodes", 0 );
        routeServerNodes.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                if ( !valueChangeEvent.getProperty().getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            valueChangeEvent.getProperty().getValue().toString().replace( "[", "" ).replace( "]", "" )
                                            .split( "," );
                    BeanContainer<String, MongoRouterNode> container =
                            ( BeanContainer<String, MongoRouterNode> ) routeServerNodes.getContainerDataSource();
                    Set<MongoRouterNode> mongoDataNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<MongoRouterNode> mongoBean = container.getItem( node.trim() );
                        MongoRouterNode dataNode = mongoBean.getBean();
                        mongoDataNodes.add( dataNode );
                    }
                    wizard.getMongoClusterConfig().setRouterServers( mongoDataNodes );
                }
            }
        } );

        //datanodes number
        dataServerNodes = createTwinColSelect( "Select data server node.", "Available data server nodes",
                "Selected data server nodes", 0 );
        dataServerNodes.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                if ( !valueChangeEvent.getProperty().getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            valueChangeEvent.getProperty().getValue().toString().replace( "[", "" ).replace( "]", "" )
                                            .split( "," );
                    BeanContainer<String, MongoDataNode> container =
                            ( BeanContainer<String, MongoDataNode> ) dataServerNodes.getContainerDataSource();
                    Set<MongoDataNode> mongoDataNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<MongoDataNode> mongoBean = container.getItem( node.trim() );
                        MongoDataNode dataNode = mongoBean.getBean();
                        mongoDataNodes.add( dataNode );
                    }
                    wizard.getMongoClusterConfig().setDataNodes( mongoDataNodes );
                }
            }
        } );

        TextField replicaSetName = new TextField( "Enter replica set name" );
        replicaSetName.setInputPrompt( wizard.getMongoClusterConfig().getReplicaSetName() );
        replicaSetName.setMaxLength( 20 );
        replicaSetName.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( !Strings.isNullOrEmpty( value ) )
                {
                    wizard.getMongoClusterConfig().setReplicaSetName( value );
                }
            }
        } );

        TextField cfgSrvPort = new TextField( "Enter port for configuration servers" );
        cfgSrvPort.setInputPrompt( wizard.getMongoClusterConfig().getCfgSrvPort() + "" );
        cfgSrvPort.setMaxLength( 5 );
        cfgSrvPort.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( StringUtil.isNumeric( value ) )
                {
                    wizard.getMongoClusterConfig().setCfgSrvPort( Integer.parseInt( value ) );
                }
            }
        } );

        TextField routerPort = new TextField( "Enter port for routers" );
        routerPort.setInputPrompt( wizard.getMongoClusterConfig().getRouterPort() + "" );
        routerPort.setMaxLength( 5 );
        routerPort.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( StringUtil.isNumeric( value ) )
                {
                    wizard.getMongoClusterConfig().setRouterPort( Integer.parseInt( value ) );
                }
            }
        } );

        TextField dataNodePort = new TextField( "Enter port for data nodes" );
        dataNodePort.setInputPrompt( wizard.getMongoClusterConfig().getDataNodePort() + "" );
        dataNodePort.setMaxLength( 5 );
        dataNodePort.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( StringUtil.isNumeric( value ) )
                {
                    wizard.getMongoClusterConfig().setDataNodePort( Integer.parseInt( value ) );
                }
            }
        } );

        TextField domain = new TextField( "Enter domain name" );
        domain.setInputPrompt( wizard.getMongoClusterConfig().getDomainName() );
        domain.setMaxLength( 20 );
        domain.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( !Strings.isNullOrEmpty( value ) )
                {
                    wizard.getMongoClusterConfig().setDomainName( value );
                }
            }
        } );

        Button next = new Button( "Next" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( envList.getValue() == null )
                {
                    show( "Select environment to configure." );
                    return;
                }

                if ( Strings.isNullOrEmpty( wizard.getMongoClusterConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name" );
                    return;
                }
                wizard.next();
            }
        } );

        Button back = new Button( "Back" );
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

        content.addComponent( envList );
        content.addComponent( clusterNameTxtFld );
        content.addComponent( replicaSetName );
        content.addComponent( domain );
        content.addComponent( cfgSrvPort );
        content.addComponent( routerPort );
        content.addComponent( dataNodePort );
        content.addComponent( new Label() );
        content.addComponent( new Label() );
        content.addComponent( configServerNodes );
        content.addComponent( routeServerNodes );
        content.addComponent( dataServerNodes );
        content.addComponent( new Label() );
        content.addComponent( buttons );
        content.setComponentAlignment( buttons, Alignment.MIDDLE_CENTER );
        content.addComponent( new Label() );

        addComponent( layout );
    }


    private ComboBox getEnvironmentList( final Wizard wizard )
    {
        List<MongoClusterConfig> environments = wizard.getMongo().getClusters();

        final BeanContainer<String, MongoClusterConfig> container = new BeanContainer<>( MongoClusterConfig.class );
        container.setBeanIdProperty( "clusterName" );

        container.addAll( environments );

        ComboBox envList = new ComboBox( "Select environment" );
        envList.setItemCaptionPropertyId( "clusterName" );
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
                BeanItem<MongoClusterConfig> clusterConfig =
                        container.getItem( valueChangeEvent.getProperty().getValue() );

                MongoClusterConfig mongoClusterConfig = clusterConfig.getBean();
                wizard.getMongoClusterConfig().setEnvironmentId( mongoClusterConfig.getEnvironmentId() );
                wizard.getMongoClusterConfig().setCfgSrvPort( mongoClusterConfig.getCfgSrvPort() );
                wizard.getMongoClusterConfig().setDataNodePort( mongoClusterConfig.getDataNodePort() );
                wizard.getMongoClusterConfig().setRouterPort( mongoClusterConfig.getRouterPort() );
                wizard.getMongoClusterConfig().setDomainName( mongoClusterConfig.getDomainName() );
                wizard.getMongoClusterConfig().setReplicaSetName( mongoClusterConfig.getReplicaSetName() );
                wizard.getMongoClusterConfig().setNumberOfConfigServers( 1 );
                wizard.getMongoClusterConfig().setNumberOfDataNodes( 3 );
                wizard.getMongoClusterConfig().setNumberOfRouters( 1 );


                fillConfigServers( configServerNodes, mongoClusterConfig.getAllNodes(), MongoNode.class );
                fillConfigServers( routeServerNodes, mongoClusterConfig.getAllNodes(), MongoNode.class );
                fillConfigServers( dataServerNodes, mongoClusterConfig.getAllNodes(), MongoNode.class );
            }
        } );

        return envList;
    }


    private <T> void fillConfigServers( TwinColSelect twinColSelect, Set<T> configServers, Class<T> clazz )
    {
        BeanContainer<String, T> container = new BeanContainer<>( clazz );
        container.setBeanIdProperty( "hostname" );
        container.addAll( configServers );

        twinColSelect.setContainerDataSource( container );
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


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
