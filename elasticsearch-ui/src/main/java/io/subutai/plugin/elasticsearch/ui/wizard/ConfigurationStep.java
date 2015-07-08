package io.subutai.plugin.elasticsearch.ui.wizard;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.PeerException;
import io.subutai.common.settings.Common;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.server.Sizeable;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;


public class ConfigurationStep extends VerticalLayout
{
    public static final String PACKAGE_NAME =
            Common.PACKAGE_PREFIX + ElasticsearchClusterConfiguration.PRODUCT_KEY.toLowerCase();


    public ConfigurationStep( final EnvironmentWizard environmentWizard )
    {
        removeAllComponents();
        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        final TextField clusterNameTxtFld = new TextField( "Enter cluster name" );
        clusterNameTxtFld.setId( "ElasticSearchConfClusterName" );
        clusterNameTxtFld.setInputPrompt( "Cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setValue( environmentWizard.getConfig().getClusterName() );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                environmentWizard.getConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        final Set<Environment> environmentList = environmentWizard.getEnvironmentManager().getEnvironments();
        List<Environment> envList = new ArrayList<>();
        for ( Environment anEnvironmentList : environmentList )
        {
            boolean exists = isTemplateExists( anEnvironmentList.getContainerHosts() );
            if ( exists )
            {
                envList.add( anEnvironmentList );
            }
        }

        // all nodes
        final TwinColSelect allNodesSelect =
                getTwinSelect( "Nodes to be configured", "Available Nodes", "Selected Nodes", 4 );
        allNodesSelect.setId( "AllNodes" );
        allNodesSelect.setValue( environmentWizard.getConfig().getNodes() );
        allNodesSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
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
                    environmentWizard.getConfig().getNodes().clear();
                    environmentWizard.getConfig().getNodes().addAll( nodes );
                }
            }
        } );

        final ComboBox envCombo = new ComboBox( "Choose environment" );
        envCombo.setId( "envCombo" );
        BeanItemContainer<Environment> eBean = new BeanItemContainer<>( Environment.class );
        eBean.addAll( envList );
        envCombo.setContainerDataSource( eBean );
        envCombo.setNullSelectionAllowed( false );
        envCombo.setTextInputAllowed( false );
        envCombo.setItemCaptionPropertyId( "name" );
        envCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Environment e = ( Environment ) event.getProperty().getValue();
                environmentWizard.getConfig().setEnvironmentId( e.getId() );

                allNodesSelect.setValue( null );
                environmentWizard.getConfig().getNodes().clear();


                for ( ContainerHost host : filterEnvironmentContainers( e.getContainerHosts(), environmentWizard, e ) )
                {
                    allNodesSelect.addItem( host );
                    allNodesSelect.setItemCaption( host,
                            ( host.getHostname() + " (" + host.getIpByInterfaceName( "eth0" ) + ")" ) );
                }
            }
        } );

        Button next = new Button( "Next" );
        next.setId( "ElasticSearchConfNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( environmentWizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name !" );
                }
                else if ( environmentWizard.getConfig().getNodes().isEmpty() )
                {
                    show( "Please select nodes to be configured !" );
                }
                else
                {
                    environmentWizard.next();
                }
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "ElasticSearchConfBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                environmentWizard.back();
            }
        } );

        VerticalLayout layout = new VerticalLayout();
        layout.setSpacing( true );
        layout.addComponent( new Label( "Please, specify installation settings" ) );
        layout.addComponent( content );

        HorizontalLayout buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( next );

        content.addComponent( clusterNameTxtFld );
        content.addComponent( envCombo );
        content.addComponent( allNodesSelect );
        content.addComponent( buttons );

        addComponent( layout );
    }


    private boolean isTemplateExists( Set<ContainerHost> containerHosts )
    {
        for ( ContainerHost host : containerHosts )
        {
            try
            {
                if ( host.getTemplate().getProducts().contains( ElasticsearchClusterConfiguration.PACKAGE_NAME ) )
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


    private Set<ContainerHost> filterEnvironmentContainers( Set<ContainerHost> containerHosts, EnvironmentWizard environmentWizard, Environment environment )
    {
        Set<ContainerHost> filteredSet = new HashSet<>();
        for ( ContainerHost containerHost : containerHosts )
        {
            try
            {
                if ( containerHost.getTemplate().getProducts()
                                  .contains( ElasticsearchClusterConfiguration.PACKAGE_NAME ) )
                {
                    filteredSet.add( containerHost );
                }
            }
            catch ( PeerException e )
            {
                e.printStackTrace();
            }
        }

        //exclude nodes which are in other clusters
        List<ElasticsearchClusterConfiguration> configs = environmentWizard.getElasticsearch().getClusters();
        for( ElasticsearchClusterConfiguration config : configs )
        {
            if( !config.getEnvironmentId().equals( environment.getId() ))
            {
                continue;
            }
            for( Iterator<ContainerHost> iterator = filteredSet.iterator(); iterator.hasNext(); )
            {
                ContainerHost node = iterator.next();
                if( config.getNodes().contains( node.getId() ))
                {
                    iterator.remove();
                }
            }
        }


        return filteredSet;
    }


    public static TwinColSelect getTwinSelect( String title, String leftTitle, String rightTitle, int rows )
    {
        TwinColSelect twinColSelect = new TwinColSelect( title );
        twinColSelect.setRows( rows );
        twinColSelect.setMultiSelect( true );
        twinColSelect.setImmediate( true );
        twinColSelect.setLeftColumnCaption( leftTitle );
        twinColSelect.setRightColumnCaption( rightTitle );
        twinColSelect.setWidth( 100, Sizeable.Unit.PERCENTAGE );
        twinColSelect.setRequired( true );
        return twinColSelect;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
