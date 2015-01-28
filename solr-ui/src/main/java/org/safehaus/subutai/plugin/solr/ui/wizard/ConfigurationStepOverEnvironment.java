/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.solr.ui.wizard;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.Environment;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanContainer;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;


public class ConfigurationStepOverEnvironment extends VerticalLayout
{

    public ConfigurationStepOverEnvironment( final Wizard wizard )
    {

        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );


        final ComboBox envList = getEnvironmentList( wizard );

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
        content.addComponent( buttons );

        addComponent( layout );
    }


    private ComboBox getEnvironmentList( final Wizard wizard )
    {
        List<Environment> environments = new ArrayList<>( wizard.getEnvironmentManager().getEnvironments() );
        for ( int i = 0; i < environments.size(); i++ )
        {
            boolean applicable = false;
            for ( final ContainerHost containerHost : environments.get( i ).getContainerHosts() )
            {
                if ( containerHost.getTemplateName().equalsIgnoreCase( SolrClusterConfig.PRODUCT_KEY ) )
                {
                    applicable = true;
                }
            }
            if ( !applicable )
            {
                environments.remove( i );
                i--;
            }
        }

        BeanContainer<String, Environment> container = new BeanContainer<>( Environment.class );
        container.setBeanIdProperty( "id" );

        for ( final Environment environment : environments )
        {
            container.addBean( environment );
        }

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
                UUID envId = ( UUID ) valueChangeEvent.getProperty().getValue();
                wizard.getSolrClusterConfig().setEnvironmentId( envId );
            }
        } );

        return envList;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
