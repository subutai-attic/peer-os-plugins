package org.safehaus.subutai.plugin.storm.ui.wizard;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.server.Sizeable;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;


public class ConfigurationStep extends Panel
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigurationStep.class );
    private EnvironmentManager environmentManager;
    private GridLayout installationControls;
    private TextField clusterNameTxtFld;
    private TextField domainNameTxtFld;
    private Button next;
    private HorizontalLayout buttons;
    private ComboBox envCombo;
    private ComboBox nimbusNode;
    private TwinColSelect allNodesSelect;
    private Wizard wizard;
    private Zookeeper zookeeper;
    private Component nimbusElem;
    private HorizontalLayout hl;


    public ConfigurationStep( final Zookeeper zookeeper, final Wizard wizard,
                              final EnvironmentManager environmentManager )
    {
        this.wizard = wizard;
        this.zookeeper = zookeeper;
        this.environmentManager = environmentManager;

        installationControls = new GridLayout( 1, 3 );
        installationControls.setSizeFull();
        installationControls.setSpacing( true );
        installationControls.setMargin( true );

        clusterNameTxtFld = new TextField( "Enter cluster name" );
        clusterNameTxtFld.setId( "StormConfClusterName" );
        clusterNameTxtFld.setInputPrompt( "Cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        domainNameTxtFld = new TextField( "Enter domain name" );
        domainNameTxtFld.setId( "domainNameTxtFld" );
        domainNameTxtFld.setInputPrompt( "intra.lan" );
        domainNameTxtFld.setRequired( true );
        domainNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getConfig().setDomainName( event.getProperty().getValue().toString().trim() );
            }
        } );

        // list of environment
        final List<Environment> environmentList = new ArrayList<>( wizard.getEnvironmentManager().getEnvironments() );
        List<Environment> envList = new ArrayList<>();
        for ( Environment anEnvironmentList : environmentList )
        {
            boolean exists =
                    isTemplateExists( anEnvironmentList.getContainerHosts(), StormClusterConfiguration.TEMPLATE_NAME );
            if ( exists )
            {
                envList.add( anEnvironmentList );
            }
        }


        envCombo = new ComboBox( "Choose environment" );
        envCombo.setId( "envCombo" );
        envCombo.setNullSelectionAllowed( true );
        envCombo.setTextInputAllowed( false );
        envCombo.setItemCaptionPropertyId( "name" );
        BeanItemContainer<Environment> eBean = new BeanItemContainer<>( Environment.class );
        eBean.addAll( envList );
        envCombo.setContainerDataSource( eBean );
        envCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Environment environment = ( Environment ) event.getProperty().getValue();
                if ( environment != null )
                {
                    wizard.getConfig().setEnvironmentId( environment.getId() );
                    nimbusNode.removeAllItems();
                    nimbusNode.setValue( null );

                    for ( ContainerHost host : filterEnvironmentContainers( environment.getContainerHosts() ) )
                    {
                        allNodesSelect.addItem( host.getId() );
                        allNodesSelect.setItemCaption( host.getId(),
                                ( host.getHostname() + " (" + host.getIpByInterfaceName( "eth0" ) + ")" ) );
                        nimbusNode.addItem( host.getId() );
                        nimbusNode.setItemCaption( host.getId(),
                                ( host.getHostname() + " (" + host.getIpByInterfaceName( "eth0" ) + ")" ) );
                    }
                }
                else
                {
                    allNodesSelect.removeAllItems();
                    nimbusNode.removeAllItems();
                }
                Environment env;
                try
                {
                    env = environmentManager.findEnvironment( wizard.getConfig().getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    LOGGER.error( "Environment not found with id: " + wizard.getConfig().getEnvironmentId().toString(),
                            e );
                    return;
                }
                fillUpComboBox( allNodesSelect, env );
            }
        } );


        nimbusNode = new ComboBox( "Choose Nimbus Node" );
        nimbusNode.setId( "nimbusNode" );
        nimbusNode.setNullSelectionAllowed( false );
        nimbusNode.setTextInputAllowed( false );
        nimbusNode.setImmediate( true );
        nimbusNode.setRequired( true );
        nimbusNode.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Environment env;
                try
                {
                    env = environmentManager.findEnvironment( wizard.getConfig().getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    LOGGER.error( "Environment not found with id: " + wizard.getConfig().getEnvironmentId().toString(),
                            e );
                    return;
                }
                fillUpComboBox( allNodesSelect, env );
                UUID uuid = ( UUID ) event.getProperty().getValue();
                ContainerHost containerHost = null;
                try
                {
                    containerHost = env.getContainerHostById( uuid );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container host not found with id: " + uuid.toString(), e );
                    return;
                }
                wizard.getConfig().setNimbus( containerHost.getId() );
                allNodesSelect.removeItem( containerHost );
            }
        } );


        allNodesSelect =
                getTwinSelect( "Nodes to be configured as Supervisor", "Available Nodes", "Selected Nodes", 4 );
        allNodesSelect.setId( "AllNodes" );
        allNodesSelect.setValue( null );
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
                    wizard.getConfig().setSupervisors( nodes );
                }
            }
        } );


        next = new Button( "Next" );
        next.setId( "ZookeeperConfNext" );
        next.addStyleName( "default" );

        Button back = new Button( "Back" );
        back.setId( "ZookeeperConfBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.back();
            }
        } );

        buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( next );

        if ( !wizard.getConfig().isExternalZookeeper() )
        {
            embeddedInstallation( wizard );
        }
        else
        {
            externalInstallation( wizard );
        }
    }


    private void externalInstallation( final Wizard wizard )
    {
        ComboBox zkClustersCombo = new ComboBox( "Zookeeper cluster" );
        zkClustersCombo.setId( "StormConfClusterCombo" );
        final ComboBox masterNodeCombo = makeMasterNodeComboBox( wizard );
        masterNodeCombo.setId( "StormMasterNodeCombo" );

        zkClustersCombo.setImmediate( true );
        zkClustersCombo.setTextInputAllowed( false );
        zkClustersCombo.setRequired( true );
        zkClustersCombo.setNullSelectionAllowed( false );
        zkClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent e )
            {
                masterNodeCombo.removeAllItems();
                if ( e.getProperty().getValue() != null )
                {
                    ZookeeperClusterConfig zookeeperClusterConfig =
                            ( ZookeeperClusterConfig ) e.getProperty().getValue();

                    Environment zookeeperEnvironment = null;
                    try
                    {
                        zookeeperEnvironment = wizard.getEnvironmentManager()
                                                     .findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e1 )
                    {
                        LOGGER.error( "Environment not found with id: " + zookeeperClusterConfig.getEnvironmentId()
                                                                                                .toString(), e );
                        return;
                    }
                    Set<ContainerHost> zookeeperNodes = Sets.newHashSet();
                    try
                    {
                        for ( UUID nodeId : filterZKNodes( zookeeperClusterConfig.getNodes() ) )
                        {
                            zookeeperNodes.add( zookeeperEnvironment.getContainerHostById( nodeId ) );
                        }
                    }
                    catch ( ContainerHostNotFoundException e1 )
                    {
                        LOGGER.error( "Some container hosts not found by ids: " + zookeeperClusterConfig.getNodes()
                                                                                                        .toString(),
                                e1 );
                        return;
                    }
                    for ( ContainerHost containerHost : zookeeperNodes )
                    {
                        masterNodeCombo.addItem( containerHost );
                        masterNodeCombo.setItemCaption( containerHost, containerHost.getHostname() );
                    }
                    // do select if values exist
                    if ( wizard.getConfig().getNimbus() != null )
                    {
                        masterNodeCombo.setValue( wizard.getConfig().getNimbus() );
                    }

                    wizard.setZookeeperClusterConfig( zookeeperClusterConfig );

                    wizard.getConfig().setZookeeperClusterName( zookeeperClusterConfig.getClusterName() );
                }
            }
        } );
        List<ZookeeperClusterConfig> zk_list = zookeeper.getClusters();
        for ( ZookeeperClusterConfig zkc : zk_list )
        {
            zkClustersCombo.addItem( zkc );
            zkClustersCombo.setItemCaption( zkc, zkc.getClusterName() );
            if ( zkc.getClusterName().equals( wizard.getConfig().getZookeeperClusterName() ) )
            {
                zkClustersCombo.setValue( zkc );
            }
        }
        if ( wizard.getConfig().getNimbus() != null )
        {
            masterNodeCombo.setValue( wizard.getConfig().getNimbus() );
        }
        nimbusElem = new Panel( "Nimbus node", hl );
        nimbusElem.setSizeUndefined();
        nimbusElem.setStyleName( "default" );


        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name !" );
                }
                else if ( Strings.isNullOrEmpty( wizard.getConfig().getDomainName() ) )
                {
                    show( "Please provide domain name !" );
                }
                else if ( wizard.getConfig().getNimbus() == null )
                {
                    show( "Please select nimbus node!" );
                }
                else if ( wizard.getConfig().getSupervisors().size() <= 0 )
                {
                    show( "Please select supervisor to be configured !" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        installationControls.addComponent(
                new Label( "Please, specify installation settings for embedded Zookeeper cluster installation" ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( domainNameTxtFld );
        installationControls.addComponent( envCombo );
        installationControls.addComponent( zkClustersCombo );
        installationControls.addComponent( masterNodeCombo );
        installationControls.addComponent( allNodesSelect );
        installationControls.addComponent( buttons );

        setContent( installationControls );
    }


    private ComboBox makeMasterNodeComboBox( final Wizard wizard )
    {
        ComboBox cb = new ComboBox( "Chose Nimbus Node" );

        cb.setId( "StormConfMasterNodes" );
        cb.setImmediate( true );
        cb.setTextInputAllowed( false );
        cb.setRequired( true );
        cb.setNullSelectionAllowed( false );
        cb.addValueChangeListener( new Property.ValueChangeListener()
        {

            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                ContainerHost serverNode = ( ContainerHost ) event.getProperty().getValue();
                wizard.getConfig().setNimbus( serverNode.getId() );
            }
        } );
        return cb;
    }


    private void embeddedInstallation( final Wizard wizard )
    {
        installationControls.addComponent(
                new Label( "Please, specify installation settings for embedded Zookeeper cluster installation" ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( domainNameTxtFld );
        installationControls.addComponent( envCombo );
        installationControls.addComponent( nimbusNode );
        installationControls.addComponent( allNodesSelect );
        installationControls.addComponent( buttons );

        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name !" );
                }
                else if ( Strings.isNullOrEmpty( wizard.getConfig().getDomainName() ) )
                {
                    show( "Please provide domain name !" );
                }
                else if ( wizard.getConfig().getNimbus() == null )
                {
                    show( "Please select nimbus node!" );
                }
                else if ( wizard.getConfig().getSupervisors().size() <= 0 )
                {
                    show( "Please select supervisor to be configured !" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        setContent( installationControls );
    }


    private void show( String notification )
    {
        Notification.show( notification );
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


    private Set<ContainerHost> filterEnvironmentContainers( Set<ContainerHost> containerHosts )
    {
        Set<ContainerHost> filteredSet = new HashSet<>();
        List<UUID> stormNodes = new ArrayList<>();
        for ( StormClusterConfiguration stormConfig : wizard.getStormManager().getClusters() )
        {
            stormNodes.addAll( stormConfig.getAllNodes() );
        }
        for ( ContainerHost containerHost : containerHosts )
        {
            if ( ( containerHost.getTemplateName().equals( StormClusterConfiguration.TEMPLATE_NAME ) ) && !( stormNodes
                    .contains( containerHost.getId() ) ) )
            {
                filteredSet.add( containerHost );
            }
        }
        return filteredSet;
    }


    private void fillUpComboBox( TwinColSelect target, Environment environment )
    {
        if ( environment != null )
        {
            wizard.getConfig().setEnvironmentId( environment.getId() );
            target.removeAllItems();
            target.setValue( null );

            for ( ContainerHost host : filterEnvironmentContainers( environment.getContainerHosts() ) )
            {
                target.addItem( host );
                target.setItemCaption( host,
                        ( host.getHostname() + " (" + host.getIpByInterfaceName( "eth0" ) + ")" ) );
            }
        }
        else
        {
            target.removeAllItems();
        }
    }

    //exclude nodes that are already in another zookeeper cluster
    private List<UUID> filterZKNodes( Set<UUID> zookeeperNodes )
    {
        List<UUID> zkNodes = new ArrayList<>();
        List<UUID> filteredNodes = new ArrayList<>();
        for ( StormClusterConfiguration stormConfig : wizard.getStormManager().getClusters() )
        {
            zkNodes.addAll( stormConfig.getAllNodes() );
        }
        for ( UUID node : zookeeperNodes )
        {
            if ( !zkNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }

}
