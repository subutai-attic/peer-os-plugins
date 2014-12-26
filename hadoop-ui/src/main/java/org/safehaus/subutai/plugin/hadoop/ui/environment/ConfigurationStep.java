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

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.hostregistry.api.HostRegistry;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;

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

    private static final int MAX_NUMBER_OF_NODES_PER_SERVER = 5;
    private static final String SUGGESTED_NUMBER_OF_NODES_CAPTION = " (Suggested)";
    private EnvironmentManager environmentManager;
    private EnvironmentWizard environmentWizard;
    private Hadoop hadoop;


    public ConfigurationStep( final EnvironmentWizard wizard, HostRegistry hostRegistry,
                              final EnvironmentManager environmentManager, final Hadoop hadoop )

    {
        this.environmentManager = environmentManager ;
        this.environmentWizard = wizard;
        this.hadoop = hadoop;

        setSizeFull();
        GridLayout content = new GridLayout( 2, 7 );
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

        final List<Environment> environmentList = wizard.getEnvironmentManager().getEnvironments();
        List<Environment> envList = new ArrayList<>();
        for ( Environment anEnvironmentList : environmentList )
        {
            boolean exists = isTemplateExists( anEnvironmentList.getContainerHosts(), "hadoop" );
            if ( exists )
            {
                envList.add( anEnvironmentList );
            }
        }


        final ComboBox nameNodeCombo = new ComboBox( "Choose NameNode" );
        BeanItemContainer<Environment> eBeanNameNode = new BeanItemContainer<>( Environment.class );
        eBeanNameNode.addAll( envList );
        nameNodeCombo.setContainerDataSource( eBeanNameNode );
        nameNodeCombo.setNullSelectionAllowed( false );
        nameNodeCombo.setTextInputAllowed( false );
        nameNodeCombo.setItemCaptionPropertyId( "name" );
        nameNodeCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Environment e = ( Environment ) event.getProperty().getValue();
                wizard.getConfig().setEnvironmentId( e.getId() );
            }
        } );

        final ComboBox envCombo = new ComboBox( "Choose environment" );
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
                wizard.getConfig().setEnvironmentId( e.getId() );
                setComboDS( nameNodeCombo, e.getContainerHosts() );

            }
        } );




        //        // configuration servers number
        //        List<String> slaveNodeCountList = new ArrayList<String>();
        //        // TODO please do not count only local resource hosts since environments can span multiple peers
        //        // remove host registry usage once this fix is applied
        //        int connected_fai_count = hostRegistry.getResourceHostsInfo().size() - 1;
        //        for ( int i = 1; i <= ( connected_fai_count ) * MAX_NUMBER_OF_NODES_PER_SERVER; i++ )
        //        {
        //            if ( i == connected_fai_count )
        //            {
        //                slaveNodeCountList.add( i + SUGGESTED_NUMBER_OF_NODES_CAPTION );
        //            }
        //            else
        //            {
        //                slaveNodeCountList.add( i + "" );
        //            }
        //        }

        //        ComboBox slaveNodesComboBox = new ComboBox( "Choose number of slave nodes", slaveNodeCountList );
        //        slaveNodesComboBox.setId( "HadoopSlavesNodeComboBox" );
        //        slaveNodesComboBox.setImmediate( true );
        //        slaveNodesComboBox.setTextInputAllowed( false );
        //        slaveNodesComboBox.setNullSelectionAllowed( false );
        //        slaveNodesComboBox.setValue( wizard.getHadoopClusterConfig().getCountOfSlaveNodes() );
        //
        //        // parse count of slave nodes input field
        //        slaveNodesComboBox.addValueChangeListener( new Property.ValueChangeListener()
        //        {
        //            @Override
        //            public void valueChange( Property.ValueChangeEvent event )
        //            {
        //                String slaveNodeComboBoxSelection = event.getProperty().getValue().toString();
        //                int slaveNodeCount;
        //                int suggestedNumberOfNodesCaptionStart =
        //                        slaveNodeComboBoxSelection.indexOf( SUGGESTED_NUMBER_OF_NODES_CAPTION.charAt( 0 ) );
        //                if ( suggestedNumberOfNodesCaptionStart < 0 )
        //                {
        //                    slaveNodeCount = Integer.parseInt( slaveNodeComboBoxSelection );
        //                }
        //                else
        //                {
        //                    slaveNodeCount = Integer.parseInt(
        //                            slaveNodeComboBoxSelection.substring( 0, suggestedNumberOfNodesCaptionStart ) );
        //                }
        //                wizard.getHadoopClusterConfig().setCountOfSlaveNodes( slaveNodeCount );
        //            }
        //        } );


        //configuration replication factor
        ComboBox replicationFactorComboBox =
                new ComboBox( "Choose replication factor for slave nodes", Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ) );
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
                      .setReplicationFactor( Integer.parseInt( ( String ) event.getProperty().getValue() ) );
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
        //        content.addComponent( slaveNodesComboBox );
        content.addComponent( replicationFactorComboBox );
        content.addComponent( nameNodeCombo );

        content.addComponent( buttons );

        addComponent( layout );
    }



    private void setComboDS( ComboBox target, Set<ContainerHost> hosts )
    {
        target.removeAllItems();
        target.setValue( null );
        for ( ContainerHost host : hosts )
        {
            target.addItem( host.getId() );
            target.setItemCaption( host.getId(), host.getHostname() );
        }
    }


    private ContainerHost getHost( UUID uuid )
    {
        return environmentManager.getEnvironmentByUUID( environmentWizard.getConfig().
                getEnvironmentId() ).getContainerHostById( uuid );
    }

    private boolean isTemplateExists( Set<ContainerHost> containerHosts, String templateName ){
        for ( ContainerHost host: containerHosts ){
            if ( host.getTemplateName().equals( templateName ) ){
                return true;
            }
        }
        return  false;
    }


    public static TwinColSelect getTwinSelect( String title, String captionProperty, String leftTitle,
                                               String rightTitle, int rows )
    {
        TwinColSelect twinColSelect = new TwinColSelect( title );
        twinColSelect.setItemCaptionPropertyId( captionProperty );
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
