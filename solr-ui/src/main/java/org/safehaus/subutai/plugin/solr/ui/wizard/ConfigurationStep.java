/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.solr.ui.wizard;


import java.util.List;

import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;


public class ConfigurationStep extends VerticalLayout
{

    public ConfigurationStep( final Wizard wizard )
    {

        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        List<Environment> environments = wizard.getEnvironmentManager().getEnvironments();
        for ( final Environment environment : environments )
        {
            boolean applicable = false;
            for ( final ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( containerHost.getTemplateName().equalsIgnoreCase( SolrClusterConfig.PRODUCT_KEY ) )
                {
                    applicable = true;
                }
            }
            if ( !applicable )
            {
                environments.remove( environment );
            }
        }

        ComboBox envList = new ComboBox( "Select environment", environments );
        envList.setImmediate( true );
        envList.setNullSelectionAllowed( false );
        envList.setTextInputAllowed( false );
        envList.setNullSelectionAllowed( false );
        envList.setWidth( 150, Unit.PIXELS );

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
                if ( Strings.isNullOrEmpty( wizard.getSolrClusterConfig().getClusterName() ) )
                {
                    show( "Please provide installation name" );
                }
                else
                {
                    wizard.next();
                }
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
        layout.addComponent( new Label( "Please, Talas specify installation settings" ) );
        layout.addComponent( content );

        HorizontalLayout buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( next );

        content.addComponent( clusterNameTxtFld );
        content.addComponent( buttons );
        content.addComponent( envList );

        addComponent( layout );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
