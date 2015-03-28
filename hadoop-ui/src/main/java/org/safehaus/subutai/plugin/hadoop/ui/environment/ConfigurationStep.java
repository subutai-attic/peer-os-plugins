/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.hadoop.ui.environment;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.hostregistry.api.HostRegistry;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

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
    public ConfigurationStep( final EnvironmentWizard wizard, HostRegistry hostRegistry,
                              final EnvironmentManager environmentManager )
    {
        setSizeFull();
        GridLayout content = new GridLayout( 1, 7 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        final TextField clusterNameTxtFld = new TextField( "Enter cluster name" );
        clusterNameTxtFld.setId( "HadoopClusterNameTxtField" );
        clusterNameTxtFld.setInputPrompt( "Cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setMaxLength( 20 );
        if ( !Strings.isNullOrEmpty( wizard.getHadoopClusterConfig().getClusterName() ) )
        {
            clusterNameTxtFld.setValue( wizard.getHadoopClusterConfig().getClusterName() );
        }
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getHadoopClusterConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        final List<Environment> environmentList = new ArrayList<>( wizard.getEnvironmentManager().getEnvironments() );
        List<Environment> envList = new ArrayList<>();
        for ( Environment anEnvironmentList : environmentList )
        {
            boolean exists =
                    isTemplateExists( anEnvironmentList.getContainerHosts(), HadoopClusterConfig.TEMPLATE_NAME );
            if ( exists )
            {
                envList.add( anEnvironmentList );
            }
        }


        final ComboBox nameNodeCombo = getCombo( "Choose NameNode" );
        nameNodeCombo.setId( "nameNodeCombo" );
        nameNodeCombo.setTextInputAllowed( false );
        nameNodeCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                ContainerHost host = ( ContainerHost ) event.getProperty().getValue();
                wizard.getHadoopClusterConfig().setNameNode( host.getId() );
            }
        } );


        final ComboBox jobTracker = getCombo( "Choose JobTracker" );
        jobTracker.setId( "jobTracker" );
        jobTracker.setTextInputAllowed( false );
        jobTracker.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                ContainerHost host = ( ContainerHost ) event.getProperty().getValue();
                wizard.getHadoopClusterConfig().setJobTracker( host.getId() );
            }
        } );


        final ComboBox secNameNode = getCombo( "Choose Secondary NameNode" );
        secNameNode.setId( "secNameNode" );
        secNameNode.setTextInputAllowed( false );
        secNameNode.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                ContainerHost host = ( ContainerHost ) event.getProperty().getValue();
                wizard.getHadoopClusterConfig().setSecondaryNameNode( host.getId() );
            }
        } );


        // all nodes
        final TwinColSelect slaveNodes = getTwinSelect( "Slave Nodes", "Available Nodes", "Selected Nodes", 4 );
        slaveNodes.setId( "slaveNodes" );
        slaveNodes.addValueChangeListener( new Property.ValueChangeListener()
        {
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<UUID> nodes = new HashSet<>();
                    Set<ContainerHost> nodeList = ( Set<ContainerHost> ) event.getProperty().getValue();
                    for ( ContainerHost host : nodeList )
                    {
                        nodes.add( host.getId() );
                    }
                    wizard.getHadoopClusterConfig().setDataNodes( new ArrayList<>( nodes ) );
                    wizard.getHadoopClusterConfig().setTaskTrackers( new ArrayList<>( nodes ) );
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
                wizard.getHadoopClusterConfig().setEnvironmentId( e.getId() );
                setComboDS( nameNodeCombo, filterEnvironmentContainers( e.getContainerHosts() ) );
                setComboDS( jobTracker, filterEnvironmentContainers( e.getContainerHosts() ) );
                setComboDS( secNameNode, filterEnvironmentContainers( e.getContainerHosts() ) );
                for ( ContainerHost host : filterEnvironmentContainers( e.getContainerHosts() ) )
                {
                    slaveNodes.addItem( host );
                    slaveNodes.setItemCaption( host,
                            ( host.getHostname() + " (" + host.getIpByInterfaceName( "eth0" ) + ")" ) );
                }
            }
        } );

        //configuration replication factor
        final ComboBox replicationFactorComboBox = new ComboBox( "Choose replication factor for slave nodes",
                Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ) );
        replicationFactorComboBox.setId( "HadoopReplicationFactorComboBox" );
        replicationFactorComboBox.setImmediate( true );
        replicationFactorComboBox.setTextInputAllowed( false );
        replicationFactorComboBox.setNullSelectionAllowed( false );
        replicationFactorComboBox.setValue( wizard.getHadoopClusterConfig().getReplicationFactor() );

        replicationFactorComboBox.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getHadoopClusterConfig()
                        .setReplicationFactor( Integer.parseInt( event.getProperty().getValue().toString() ) );
            }
        } );

        TextField domain = new TextField( "Enter domain name" );
        domain.setId( "HadoopDomainTxttField" );
        domain.setInputPrompt( wizard.getHadoopClusterConfig().getDomainName() );
        domain.setValue( wizard.getHadoopClusterConfig().getDomainName() );
        domain.setMaxLength( 20 );
        domain.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                if ( !Strings.isNullOrEmpty( value ) )
                {
                    wizard.getHadoopClusterConfig().setDomainName( value );
                }
            }
        } );

        Button next = new Button( "Next" );
        next.setId( "HadoopBtnNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( wizard.getHadoopClusterConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name" );
                }
                else if ( (int) replicationFactorComboBox.getValue() > wizard.getHadoopClusterConfig().getDataNodes().size() ){
                    show( "Replication factor could NOT be bigger than slave nodes count !!!");
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "HadoopConfigBack" );
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

        content.addComponent( clusterNameTxtFld );
        content.addComponent( domain );
        content.addComponent( envCombo );
        content.addComponent( replicationFactorComboBox );
        content.addComponent( nameNodeCombo );
        content.addComponent( jobTracker );
        content.addComponent( secNameNode );
        content.addComponent( slaveNodes );
        content.addComponent( buttons );
        addComponent( layout );
    }


    public static ComboBox getCombo( String title )
    {
        ComboBox combo = new ComboBox( title );
        combo.setImmediate( true );
        combo.setTextInputAllowed( false );
        combo.setRequired( true );
        combo.setNullSelectionAllowed( false );
        return combo;
    }


    private Set<ContainerHost> filterEnvironmentContainers( Set<ContainerHost> containerHosts )
    {
        Set<ContainerHost> filteredSet = new HashSet<>();
        for ( ContainerHost containerHost : containerHosts )
        {
            if ( containerHost.getTemplateName().equals( HadoopClusterConfig.TEMPLATE_NAME ) )
            {
                filteredSet.add( containerHost );
            }
        }
        return filteredSet;
    }


    private void setComboDS( ComboBox target, Set<ContainerHost> hosts )
    {
        target.removeAllItems();
        target.setValue( null );
        for ( ContainerHost host : hosts )
        {
            target.addItem( host );
            target.setItemCaption( host, host.getHostname() );
        }
    }


    private boolean isTemplateExists( Set<ContainerHost> containerHosts, String templateName )
    {
        for ( ContainerHost host : containerHosts )
        {
            if ( host.getTemplateName().equals( templateName ) )
            {
                return true;
            }
        }
        return false;
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
