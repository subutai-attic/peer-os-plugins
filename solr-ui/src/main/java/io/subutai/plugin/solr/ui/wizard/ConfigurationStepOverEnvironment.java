/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.solr.ui.wizard;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.data.util.BeanItem;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.plugin.solr.api.SolrClusterConfig;


public class ConfigurationStepOverEnvironment extends VerticalLayout
{

    public ConfigurationStepOverEnvironment( final Wizard wizard )
    {

        setSizeFull();

        final GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );


        final TwinColSelect solrNodes = new TwinColSelect( "Available nodes" );
        solrNodes.setId( "SolrNodesSelector" );
        solrNodes.setItemCaptionPropertyId( "hostname" );
        solrNodes.setRows( 7 );
        solrNodes.setMultiSelect( true );
        solrNodes.setImmediate( true );
        solrNodes.setLeftColumnCaption( "Available Nodes" );
        solrNodes.setRightColumnCaption( "Selected Nodes" );
        solrNodes.setWidth( 100, Unit.PERCENTAGE );
        solrNodes.setRequired( true );
        solrNodes.addValueChangeListener( new Property.ValueChangeListener()
        {

            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<EnvironmentContainerHost> containerHosts =
                            new HashSet<>( ( Collection<EnvironmentContainerHost> ) event.getProperty().getValue() );
                    Set<String> containerIDs = new HashSet<>();
                    for ( EnvironmentContainerHost containerHost : containerHosts )
                    {
                        containerIDs.add( containerHost.getId() );
                    }
                    wizard.getSolrClusterConfig().setNodes( containerIDs );
                    wizard.getSolrClusterConfig().setNumberOfNodes( containerIDs.size() );
                }
            }
        } );

        final ComboBox envList = getEnvironmentList( wizard );
        envList.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                String envId = ( String ) valueChangeEvent.getProperty().getValue();
                wizard.getSolrClusterConfig().setEnvironmentId( envId );
                BeanItem environmentItem = ( BeanItem ) envList.getItem( envId );
                Environment environment = ( Environment ) environmentItem.getBean();

                Set<EnvironmentContainerHost> allowedContainerHosts = new HashSet<>( environment.getContainerHosts() );

                //Exclude container hosts which are cloned not from solr template
                for ( Iterator<EnvironmentContainerHost> it = allowedContainerHosts.iterator(); it.hasNext(); )
                {
                    EnvironmentContainerHost containerHost = it.next();
                    if ( !containerHost.getTemplateName().equalsIgnoreCase( SolrClusterConfig.TEMPLATE_NAME ) )
                    {
                        it.remove();
                    }
                }

                //Exclude container hosts which are already in solr cluster
                List<SolrClusterConfig> solrConfigs = wizard.getSolr().getClusters();
                for ( final SolrClusterConfig solrConfig : solrConfigs )
                {
                    if ( !solrConfig.getEnvironmentId().equals( environment.getId() ) )
                    {
                        continue;
                    }
                    for ( Iterator<EnvironmentContainerHost> it = allowedContainerHosts.iterator(); it.hasNext(); )
                    {
                        EnvironmentContainerHost containerHost = it.next();
                        if ( solrConfig.getNodes().contains( containerHost.getId() ) )
                        {
                            it.remove();
                        }
                    }
                }

                solrNodes.setContainerDataSource(
                        new BeanItemContainer<>( EnvironmentContainerHost.class, allowedContainerHosts ) );
            }
        } );

        final TextField clusterNameTxtFld = new TextField( "Enter installation name" );
        clusterNameTxtFld.setId( "SlrClusterNameTxtFld" );
        clusterNameTxtFld.setInputPrompt( "Installation name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setMaxLength( 20 );
        clusterNameTxtFld.setValue( wizard.getSolrClusterConfig().getClusterName() );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getSolrClusterConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );


        Button next = new Button( "Next" );
        next.setId( "SlrConfNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {

                if ( envList.getValue() == null )
                {
                    show( "Please provide environment" );
                    return;
                }

                if ( Strings.isNullOrEmpty( wizard.getSolrClusterConfig().getClusterName() ) )
                {
                    show( "Please provide installation name" );
                    return;
                }

                if ( solrNodes.getValue() == null )
                {
                    show( "Select nodes to configure solr cluster on top of it" );
                    return;
                }

                wizard.next();
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "SlrConfBack" );
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
        content.addComponent( solrNodes );
        content.addComponent( buttons );

        addComponent( layout );
    }


    private ComboBox getEnvironmentList( final Wizard wizard )
    {
        List<Environment> environments = new ArrayList<>( wizard.getEnvironmentManager().getEnvironments() );

        BeanContainer<String, Environment> container = new BeanContainer<>( Environment.class );
        container.setBeanIdProperty( "id" );
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
        //        envList.addValueChangeListener( new Property.ValueChangeListener()
        //        {
        //            @Override
        //            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
        //            {
        //                UUID envId = ( UUID ) valueChangeEvent.getProperty().getValue();
        //                wizard.getSolrClusterConfig().setEnvironmentId( envId );
        //            }
        //        } );

        return envList;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
