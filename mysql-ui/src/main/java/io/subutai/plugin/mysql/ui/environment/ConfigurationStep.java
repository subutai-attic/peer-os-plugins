package io.subutai.plugin.mysql.ui.environment;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


/**
 * Created by tkila on 5/14/15.
 */
public class ConfigurationStep extends VerticalLayout
{
    private EnvironmentWizard wizard;

    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigurationStep.class.getName() );


    public ConfigurationStep( final EnvironmentWizard environmentWizard )
    {
        this.wizard = environmentWizard;

        removeAllComponents();
        setSizeFull();

        GridLayout content = new GridLayout( 1, 3 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        final TextField clusterNameTxtFld = new TextField( "Enter cluster name" );

        clusterNameTxtFld.setId( "SQLClusterNameTxtField" );
        clusterNameTxtFld.setInputPrompt( "Cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setValue( environmentWizard.getConfig().getClusterName() );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                environmentWizard.getConfig().setClusterName( valueChangeEvent.getProperty().getValue().toString() );
            }
        } );
        final TextField domainNameTxtFld = new TextField( "Enter domain name" );
        domainNameTxtFld.setId( "MySQLDomainNameTxtField" );
        domainNameTxtFld.setInputPrompt( "intra.lan" );
        domainNameTxtFld.setRequired( true );
        domainNameTxtFld.setValue( environmentWizard.getConfig().getDomainName() );
        domainNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                environmentWizard.getConfig().setDomainName( event.getProperty().getValue().toString().trim() );
            }
        } );
        final TextField dataNodeConfDir = new TextField( "Enter Data Node(s) Config File" );
        dataNodeConfDir.setId( "dataNodeConfDir" );
        dataNodeConfDir.setInputPrompt( "Data node config file" );
        dataNodeConfDir.setRequired( true );
        dataNodeConfDir.setValue( environmentWizard.getConfig().getConfNodeDataFile() );
        dataNodeConfDir.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                environmentWizard.getConfig()
                                 .setConfNodeDataFile( valueChangeEvent.getProperty().getValue().toString().trim() );
            }
        } );
        final TextField dataNodeDataDir = new TextField( "Enter Data Node(s) Data Dir" );
        dataNodeDataDir.setId( "dataNodeDataDir" );
        dataNodeDataDir.setInputPrompt( "Data node data dir" );
        dataNodeDataDir.setRequired( true );
        dataNodeDataDir.setValue( environmentWizard.getConfig().getDataNodeDataDir() );
        dataNodeDataDir.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                environmentWizard.getConfig().setDataNodeDataDir( valueChangeEvent.getProperty().toString().trim() );
            }
        } );
        final TextField dataManNodeDir = new TextField( "Enter Manager Node(s) Data Dir" );
        dataManNodeDir.setId( "manNodeDataDir" );
        dataManNodeDir.setInputPrompt( "Manager node data dir" );
        dataManNodeDir.setRequired( true );
        dataManNodeDir.setValue( environmentWizard.getConfig().getDataManNodeDir() );
        dataManNodeDir.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                environmentWizard.getConfig().setDataManNodeDir( valueChangeEvent.getProperty().toString().trim() );
            }
        } );
        final TextField confManNodeFile = new TextField( "Enter Manager Node(s) Conf File " );
        confManNodeFile.setId( "manNodeConfFile" );
        confManNodeFile.setInputPrompt( "Manager node config file" );
        confManNodeFile.setRequired( true );
        confManNodeFile.setValue( environmentWizard.getConfig().getConfManNodeFile() );
        confManNodeFile.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                environmentWizard.getConfig().setConfManNodeFile( valueChangeEvent.getProperty().toString().trim() );
            }
        } );

        final Set<Environment> environmentSet = environmentWizard.getEnvironmentManager().getEnvironments();


        List<Environment> environmentList = new ArrayList<>();

        for ( Environment env : environmentSet )
        {
            boolean exists = isTemplateExists( env.getContainerHosts(), MySQLClusterConfig.TEMPLATE_NAME );
            if ( exists )
            {
                environmentList.add( env );
            }
        }
        final TwinColSelect managerNodeSelect =
                getTwinSelect( "Manager Node(s) to be configured", "hostname", "Available nodes", "Selected Nodes", 4 );
        managerNodeSelect.setId( "AllNodes" );
        managerNodeSelect.setValue( null );

        final TwinColSelect dataNodesSelect =
                getTwinSelect( "DataNodes", "hostname", "Available nodes", "Selected Nodes", 4 );
        dataNodesSelect.setId( "AllDataNodes" );

        final ComboBox envCombo = new ComboBox( "Choose environment" );
        envCombo.setId( "envCombo" );
        BeanItemContainer<Environment> eBean = new BeanItemContainer<Environment>( Environment.class );
        eBean.addAll( environmentList );
        envCombo.setContainerDataSource( eBean );
        envCombo.setNullSelectionAllowed( false );
        envCombo.setItemCaptionPropertyId( "name" );
        envCombo.setTextInputAllowed( false );
        envCombo.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                Environment e = ( Environment ) valueChangeEvent.getProperty().getValue();
                environmentWizard.getConfig().setEnvironmentId( e.getId() );
                managerNodeSelect.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class,
                        filterEnvironmentContainers( e.getContainerHosts() ) ) );
                dataNodesSelect.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class,
                        filterEnvironmentContainers( e.getContainerHosts() ) ) );
            }
        } );

        managerNodeSelect.addValueChangeListener( new Property.ValueChangeListener()
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
                    environmentWizard.getConfig().setManagerNodes( nodes );
                }
            }
        } );
        dataNodesSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                if ( valueChangeEvent.getProperty().getValue() != null )
                {
                    Set<UUID> nodes = new HashSet<UUID>();
                    Set<ContainerHost> nodeList = ( Set<ContainerHost> ) valueChangeEvent.getProperty().getValue();
                    for ( ContainerHost host : nodeList )
                    {
                        nodes.add( host.getId() );
                    }
                    environmentWizard.getConfig().setDataNodes( nodes );
                }
            }
        } );
        Button next = new Button( "Next" );
        next.setId( "SqlConfNextBtn" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( environmentWizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name !" );
                }
                else if ( Strings.isNullOrEmpty( environmentWizard.getConfig().getDomainName() ) )
                {
                    show( "Please provide domain name !" );
                }
                else if ( environmentWizard.getConfig().getManagerNodes().size() <= 0 )
                {
                    show( "Please select nodes to be configured !" );
                }
                else if ( environmentWizard.getConfig().getDataNodes().size() <= 0 )
                {
                    show( "Please select seeds nodes !" );
                }
                else
                {
                    environmentWizard.next();
                }
            }
        } );
        Button back = new Button( "Back" );
        back.setId( "SqlConfBackBtn" );
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
        content.addComponent( domainNameTxtFld );

        content.addComponent( dataNodeConfDir );
        content.addComponent( dataNodeDataDir );
        content.addComponent( dataManNodeDir );
        content.addComponent( confManNodeFile );

        content.addComponent( envCombo );
        content.addComponent( managerNodeSelect );
        content.addComponent( dataNodesSelect );
        content.addComponent( buttons );

        addComponent( layout );
    }


    private Set<ContainerHost> filterEnvironmentContainers( final Set<ContainerHost> containerHosts )
    {
        Set<ContainerHost> filteredSet = new HashSet<>();
        List<UUID> sqlNodes = new ArrayList<>();
        for ( MySQLClusterConfig mySQLClusterConfig : wizard.getMySQLManager().getClusters() )
        {
            sqlNodes.addAll( mySQLClusterConfig.getAllNodes() );
        }
        for ( ContainerHost containerHost : containerHosts )
        {
            if ( containerHost.getTemplateName().equals( MySQLClusterConfig.TEMPLATE_NAME ) && !( sqlNodes
                    .contains( containerHost.getId() ) ) )
            {
                filteredSet.add( containerHost );
            }
        }
        return filteredSet;
    }


    public static TwinColSelect getTwinSelect( String title, String captionProperty, String leftTitle,
                                               String rightTitle, int rows )
    {
        TwinColSelect twinColSelect = new TwinColSelect( title );
        twinColSelect.setId( "twinColSelect" );
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


    private boolean isTemplateExists( final Set<ContainerHost> containerHosts, final String templateName )
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


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
