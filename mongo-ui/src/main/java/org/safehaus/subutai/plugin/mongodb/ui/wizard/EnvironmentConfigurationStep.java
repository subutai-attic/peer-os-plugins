/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.ui.wizard;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.PeerException;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.util.StringUtil;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
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
        clusterNameTxtFld.setId( "clusterNameTxtFld" );
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
        configServerNodes.setId( "configServerNodes" );

        //routers number
        routeServerNodes =
                createTwinColSelect( "Select route nodes.", "Available route nodes", "Selected route nodes", 0 );
        routeServerNodes.setId( "routeServerNodes" );

        //datanodes number
        dataServerNodes = createTwinColSelect( "Select data server node.", "Available data server nodes",
                "Selected data server nodes", 0 );
        dataServerNodes.setId( "dataServerNodes" );

        TextField replicaSetName = new TextField( "Enter replica set name" );
        replicaSetName.setId( "replicaSetName" );
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
        cfgSrvPort.setId( "cfgSrvPort" );
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
        routerPort.setId( "routerPort" );
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
        dataNodePort.setId( "dataNodePort" );
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
        domain.setId( "domain" );
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
        next.setId( "next" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {

                LOGGER.info( configServerNodes.getValue().toString() );

                if ( !configServerNodes.getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            configServerNodes.getValue().toString().replace( "[", "" ).replace( "]", "" ).split( "," );
                    BeanContainer<String, ContainerHost> container =
                            ( BeanContainer<String, ContainerHost> ) configServerNodes.getContainerDataSource();
                    Set<UUID> mongoConfigNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<ContainerHost> mongoBean = container.getItem( node.trim() );
                        ContainerHost configNode = mongoBean.getBean();
                        mongoConfigNodes.add( configNode.getId() );
                    }
                    wizard.getMongoClusterConfig().setConfigHostIds( mongoConfigNodes );
                    wizard.setConfigServerNames( new HashSet<>( Arrays.asList( nodes ) ) );
                }
                else
                {
                    show( "Select at least one Config server." );
                    return;
                }

                LOGGER.info( routeServerNodes.getValue().toString() );
                if ( !routeServerNodes.getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            routeServerNodes.getValue().toString().replace( "[", "" ).replace( "]", "" ).split( "," );
                    BeanContainer<String, ContainerHost> container =
                            ( BeanContainer<String, ContainerHost> ) routeServerNodes.getContainerDataSource();
                    Set<UUID> mongoRouterNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<ContainerHost> mongoBean = container.getItem( node.trim() );
                        ContainerHost dataNode = mongoBean.getBean();
                        mongoRouterNodes.add( dataNode.getId() );
                    }
                    wizard.getMongoClusterConfig().setRouterHostIds( mongoRouterNodes );
                    wizard.setRouterServerNames( new HashSet<>( Arrays.asList( nodes ) ) );
                }
                else
                {
                    show( "Select at least one router server." );
                    return;
                }

                LOGGER.info( dataServerNodes.getValue().toString() );
                if ( !dataServerNodes.getValue().toString().equals( "[]" ) )
                {
                    String[] nodes =
                            dataServerNodes.getValue().toString().replace( "[", "" ).replace( "]", "" ).split( "," );

                    BeanContainer<String, ContainerHost> container =
                            ( BeanContainer<String, ContainerHost> ) dataServerNodes.getContainerDataSource();
                    Set<UUID> mongoDataNodes = new HashSet<>();
                    for ( final String node : nodes )
                    {
                        BeanItem<ContainerHost> mongoBean = container.getItem( node.trim() );
                        ContainerHost dataNode = mongoBean.getBean();
                        mongoDataNodes.add( dataNode.getId() );
                    }
                    wizard.getMongoClusterConfig().setDataServerIds( mongoDataNodes );
                    wizard.setDataServerNames( new HashSet<>( Arrays.asList( nodes ) ) );
                }
                else
                {
                    show( "Select at least one data node." );
                    return;
                }

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
        back.setId( "back" );
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
        List<Environment> environmentList = new ArrayList<>();
        for( Environment env : environments )
        {
            if( isTemplateExists( env.getContainerHosts() ))
            {
                environmentList.add( env );
            }
        }
        final BeanContainer<String, Environment> container = new BeanContainer<>( Environment.class );
        container.setBeanIdProperty( "name" );
        container.addAll( environmentList );

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

    private boolean isTemplateExists( Set<ContainerHost> containerHosts )
    {
        for ( ContainerHost host : containerHosts )
        {
            try
            {
                if ( host.getTemplate().getProducts().contains( MongoClusterConfig.PACKAGE_NAME ) )
                {
                    return true;
                }
            }
            catch ( PeerException e )
            {
                e.printStackTrace();
            }
        }
        return false;
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


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
