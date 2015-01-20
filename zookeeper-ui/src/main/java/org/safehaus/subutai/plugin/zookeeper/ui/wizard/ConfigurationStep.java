/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.zookeeper.ui.wizard;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.api.SetupType;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

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
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;


public class ConfigurationStep extends Panel
{

    private GridLayout installationControls;
    private TextField clusterNameTxtFld;
    private Button next;
    private HorizontalLayout buttons;
    private ComboBox nodesCountCombo;
    private TwinColSelect zookeeperEnvHostsSelection;


    public ConfigurationStep( final Zookeeper zookeeper, final Hadoop hadoop, final Wizard wizard,
                              EnvironmentManager environmentManager )
    {
        installationControls = new GridLayout( 1, 5 );
        installationControls.setSizeFull();
        installationControls.setSpacing( true );
        installationControls.setMargin( true );

        clusterNameTxtFld = new TextField( "Enter Zookeeper cluster name" );
        clusterNameTxtFld.setId( "ZookeeperConfClusterName" );
        clusterNameTxtFld.setInputPrompt( "Zookeeper cluster name" );
        clusterNameTxtFld.setRequired( true );
        clusterNameTxtFld.setMaxLength( 20 );
        clusterNameTxtFld.setValue( wizard.getConfig().getClusterName() );
        clusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        //number of zookeeper nodes
        nodesCountCombo = new ComboBox( "Choose number of nodes", Arrays.asList( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ) );
        nodesCountCombo.setId( "ZookeeperNumNodes" );
        nodesCountCombo.setImmediate( true );
        nodesCountCombo.setTextInputAllowed( false );
        nodesCountCombo.setNullSelectionAllowed( false );
        nodesCountCombo.setValue( wizard.getConfig().getNumberOfNodes() );

        nodesCountCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getConfig().setNumberOfNodes( ( Integer ) event.getProperty().getValue() );
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

        if ( wizard.getConfig().getSetupType() == SetupType.STANDALONE )
        {
            standaloneInstallation( wizard );
        }
        else if ( wizard.getConfig().getSetupType() == SetupType.OVER_HADOOP )
        {
            overHadoopInstallation( wizard, hadoop, environmentManager );
        }
        else if ( wizard.getConfig().getSetupType() == SetupType.WITH_HADOOP )
        {
            withHadoopInstallation( wizard );
        }
        else if ( wizard.getConfig().getSetupType() == SetupType.OVER_ENVIRONMENT )
        {
            overEnvironmentInstallation( wizard, zookeeper, environmentManager );
        }
    }


    private void withHadoopInstallation( final Wizard wizard )
    {
        //Hadoop+Zookeeper combo template based cluster installation controls

        //Hadoop settings

        final TextField hadoopClusterNameTxtFld = new TextField( "Enter Hadoop cluster name" );
        hadoopClusterNameTxtFld.setId( "ZookeeperConfHadoopCluster" );
        hadoopClusterNameTxtFld.setInputPrompt( "Hadoop cluster name" );
        hadoopClusterNameTxtFld.setRequired( true );
        hadoopClusterNameTxtFld.setMaxLength( 20 );
        if ( !Strings.isNullOrEmpty( wizard.getHadoopClusterConfig().getClusterName() ) )
        {
            hadoopClusterNameTxtFld.setValue( wizard.getHadoopClusterConfig().getClusterName() );
        }
        hadoopClusterNameTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getHadoopClusterConfig().setClusterName( event.getProperty().getValue().toString().trim() );
            }
        } );

        //configuration servers number
        List<Integer> count = new ArrayList<>();
        for ( int i = 1; i < 50; i++ )
        {
            count.add( i );
        }

        ComboBox hadoopSlaveNodesComboBox = new ComboBox( "Choose number of Hadoop slave nodes", count );
        hadoopSlaveNodesComboBox.setId( "ZookeeperConfHadoopNodesSelection" );
        hadoopSlaveNodesComboBox.setImmediate( true );
        hadoopSlaveNodesComboBox.setTextInputAllowed( false );
        hadoopSlaveNodesComboBox.setNullSelectionAllowed( false );
        hadoopSlaveNodesComboBox.setValue( wizard.getHadoopClusterConfig().getCountOfSlaveNodes() );

        hadoopSlaveNodesComboBox.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getHadoopClusterConfig().setCountOfSlaveNodes( ( Integer ) event.getProperty().getValue() );
            }
        } );

        //configuration replication factor
        ComboBox hadoopReplicationFactorComboBox =
                new ComboBox( "Choose replication factor for Hadoop slave nodes", count );
        hadoopReplicationFactorComboBox.setId( "ZookeeperConfHadoopReplFactor" );
        hadoopReplicationFactorComboBox.setImmediate( true );
        hadoopReplicationFactorComboBox.setTextInputAllowed( false );
        hadoopReplicationFactorComboBox.setNullSelectionAllowed( false );
        hadoopReplicationFactorComboBox.setValue( wizard.getHadoopClusterConfig().getReplicationFactor() );

        hadoopReplicationFactorComboBox.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                wizard.getHadoopClusterConfig().setReplicationFactor( ( Integer ) event.getProperty().getValue() );
            }
        } );

        TextField HadoopDomainTxtFld = new TextField( "Enter Hadoop cluster domain name" );
        HadoopDomainTxtFld.setId( "ZookeeperConfHadoopDomain" );
        HadoopDomainTxtFld.setInputPrompt( wizard.getHadoopClusterConfig().getDomainName() );
        HadoopDomainTxtFld.setValue( wizard.getHadoopClusterConfig().getDomainName() );
        HadoopDomainTxtFld.setMaxLength( 20 );
        HadoopDomainTxtFld.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                String value = event.getProperty().getValue().toString().trim();
                wizard.getHadoopClusterConfig().setDomainName( value );
            }
        } );


        //Zookeeper settings

        next.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {

                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide Zookeeper cluster name" );
                }
                else if ( wizard.getConfig().getNumberOfNodes() <= 0 )
                {
                    show( "Please enter number of ZK nodes" );
                }
                else if ( wizard.getConfig().getNumberOfNodes()
                        > HadoopClusterConfig.DEFAULT_HADOOP_MASTER_NODES_QUANTITY + wizard.getHadoopClusterConfig()
                                                                                           .getCountOfSlaveNodes() )
                {
                    show( "Number of ZK nodes must not exceed total number of Hadoop nodes" );
                }
                else if ( Strings.isNullOrEmpty( wizard.getHadoopClusterConfig().getClusterName() ) )
                {
                    show( "Please provide Hadoop cluster name" );
                }
                else if ( Strings.isNullOrEmpty( wizard.getHadoopClusterConfig().getDomainName() ) )
                {
                    show( "Please provide Hadoop cluster domain name" );
                }
                else if ( wizard.getHadoopClusterConfig().getCountOfSlaveNodes() <= 0 )
                {
                    show( "Please provide number of Hadoop slave nodes" );
                }
                else if ( wizard.getHadoopClusterConfig().getReplicationFactor() <= 0 )
                {
                    show( "Please provide Hadoop cluster replication factor" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        installationControls.addComponent(
                new Label( "Please, specify installation settings for combo Hadoop+Zookeeper clusters installation" ) );
        installationControls.addComponent( new Label( "Zookeeper settings" ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( nodesCountCombo );
        installationControls.addComponent( new Label( "Hadoop settings" ) );
        installationControls.addComponent( hadoopClusterNameTxtFld );
        installationControls.addComponent( hadoopSlaveNodesComboBox );
        installationControls.addComponent( hadoopReplicationFactorComboBox );
        installationControls.addComponent( HadoopDomainTxtFld );

        installationControls.addComponent( buttons );

        setContent( installationControls );
    }


    private void overHadoopInstallation( final Wizard wizard, final Hadoop hadoop,
                                         final EnvironmentManager environmentManager )
    {
        ComboBox hadoopClustersCombo = new ComboBox( "Hadoop cluster" );
        hadoopClustersCombo.setId( "ZookeeperConfHadoopCluster" );

        final TwinColSelect hadoopNodesSelect = new TwinColSelect( "Nodes", new ArrayList<ContainerHost>() );
        hadoopNodesSelect.setId( "ZookeeperConfHadoopNodesSelection" );

        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        hadoopClustersCombo.setNullSelectionAllowed( false );

        List<HadoopClusterConfig> hadoopClusterConfigs = hadoop.getClusters();
        if ( !hadoopClusterConfigs.isEmpty() )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : hadoopClusterConfigs )
            {
                hadoopClustersCombo.addItem( hadoopClusterInfo );
                hadoopClustersCombo.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        HadoopClusterConfig info = null;
        if ( wizard.getConfig().getHadoopClusterName() != null )
        {
            info = hadoop.getCluster( wizard.getConfig().getHadoopClusterName() );
        }

        if ( info != null )
        {
            hadoopClustersCombo.setValue( info );
        }
        else if ( !hadoopClusterConfigs.isEmpty() )
        {
            hadoopClustersCombo.setValue( hadoopClusterConfigs.iterator().next() );
        }

        HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) hadoopClustersCombo.getValue();

        List<UUID> allHadoopNodes = hadoopInfo.getAllNodes();
        Set<UUID> allHadoopNodeSet = new HashSet<>();
        allHadoopNodeSet.addAll( allHadoopNodes );
        Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
        final Set<ContainerHost> hadoopNodes = hadoopEnvironment.getContainerHostsByIds( allHadoopNodeSet );

        if ( hadoopClustersCombo.getValue() != null )
        {
            wizard.getConfig().setHadoopClusterName( hadoopInfo.getClusterName() );
            wizard.setHadoopClusterConfig( hadoopInfo );

            hadoopNodesSelect.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, hadoopNodes ) );
        }

        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    hadoopNodesSelect.setValue( null );
                    hadoopNodesSelect
                            .setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, hadoopNodes ) );
                    wizard.getConfig().setHadoopClusterName( hadoopInfo.getClusterName() );
                    wizard.setHadoopClusterConfig( hadoopInfo );
                    wizard.getConfig().setNodes( new HashSet<UUID>() );
                }
            }
        } );

        hadoopNodesSelect.setItemCaptionPropertyId( "hostname" );
        hadoopNodesSelect.setRows( 7 );
        hadoopNodesSelect.setMultiSelect( true );
        hadoopNodesSelect.setImmediate( true );
        hadoopNodesSelect.setLeftColumnCaption( "Available Nodes" );
        hadoopNodesSelect.setRightColumnCaption( "Selected Nodes" );
        hadoopNodesSelect.setWidth( 100, Unit.PERCENTAGE );
        hadoopNodesSelect.setRequired( true );

        if ( !CollectionUtil.isCollectionEmpty( wizard.getConfig().getNodes() ) )
        {
            hadoopNodesSelect.setValue( wizard.getConfig().getNodes() );
        }
        hadoopNodesSelect.addValueChangeListener( new Property.ValueChangeListener()
        {

            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<ContainerHost> containerHosts =
                            new HashSet<>( ( Collection<ContainerHost> ) event.getProperty().getValue() );
                    Set<UUID> containerIDs = new HashSet<>();
                    for ( ContainerHost containerHost : containerHosts )
                    {
                        containerIDs.add( containerHost.getId() );
                    }
                    wizard.getConfig().setNodes( containerIDs );
                }
            }
        } );

        next.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please, provide cluster name" );
                }
                else if ( Strings.isNullOrEmpty( wizard.getConfig().getHadoopClusterName() ) )
                {
                    show( "Please, select Hadoop cluster" );
                }
                else if ( CollectionUtil.isCollectionEmpty( wizard.getConfig().getNodes() ) )
                {
                    show( "Please, select zk nodes" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        installationControls.addComponent(
                new Label( "Please, specify installation settings for over-Hadoop cluster installation" ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( hadoopClustersCombo );
        installationControls.addComponent( hadoopNodesSelect );
        installationControls.addComponent( buttons );
        setContent( installationControls );
    }


    private void standaloneInstallation( final Wizard wizard )
    {
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {

                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        installationControls.addComponent(
                new Label( "Please, specify installation settings for standalone cluster installation" ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( nodesCountCombo );
        installationControls.addComponent( buttons );

        setContent( installationControls );
    }


    private void overEnvironmentInstallation( final Wizard wizard, Zookeeper zookeeper,
                                              EnvironmentManager environmentManager )
    {
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {

                if ( Strings.isNullOrEmpty( wizard.getConfig().getClusterName() ) )
                {
                    show( "Please provide cluster name" );
                }
                else
                {
                    wizard.next();
                }
            }
        } );

        zookeeperEnvHostsSelection = new TwinColSelect( "Environments", new ArrayList<ContainerHost>() );
        zookeeperEnvHostsSelection.setId( "ZookeeperConfHadoopNodesSelection" );
        zookeeperEnvHostsSelection.setItemCaptionPropertyId( "hostname" );
        zookeeperEnvHostsSelection.setRows( 0 );
        zookeeperEnvHostsSelection.setMultiSelect( true );
        zookeeperEnvHostsSelection.setImmediate( true );
        zookeeperEnvHostsSelection.setLeftColumnCaption( "Available Nodes" );
        zookeeperEnvHostsSelection.setRightColumnCaption( "Selected Nodes" );
        zookeeperEnvHostsSelection.setWidth( 100, Unit.PERCENTAGE );
        zookeeperEnvHostsSelection.setRequired( true );

        zookeeperEnvHostsSelection.addValueChangeListener( new Property.ValueChangeListener()
        {

            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<ContainerHost> containerHosts =
                            new HashSet<>( ( Collection<ContainerHost> ) event.getProperty().getValue() );
                    Set<UUID> containerIDs = new HashSet<>();
                    for ( ContainerHost containerHost : containerHosts )
                    {
                        containerIDs.add( containerHost.getId() );
                    }
                    wizard.getConfig().setNodes( containerIDs );
                }
            }
        } );

        installationControls.addComponent(
                new Label( "Please, specify installation settings for standalone cluster installation" ) );
        installationControls.addComponent( getEnvironmentList( wizard, zookeeper, environmentManager ) );
        installationControls.addComponent( clusterNameTxtFld );
        installationControls.addComponent( zookeeperEnvHostsSelection );
        installationControls.addComponent( buttons );

        setContent( installationControls );
    }


    private ComboBox getEnvironmentList( final Wizard wizard, Zookeeper zookeeper,
                                         EnvironmentManager environmentManager )
    {
        List<ZookeeperClusterConfig> clusterConfigs = zookeeper.getClusters();
        final Set<UUID> zookeeperContainerHosts = new HashSet<>();
        for ( final ZookeeperClusterConfig clusterConfig : clusterConfigs )
        {
            zookeeperContainerHosts.addAll( clusterConfig.getNodes() );
        }

        List<Environment> environments = new ArrayList<>( environmentManager.getEnvironments() );
        for ( int i = 0; i < environments.size(); i++ )
        {
            Environment environment = environments.get( i );
            Set<ContainerHost> envHosts = environment.getContainerHosts();
            boolean allowEnv = true;
            for ( final ContainerHost envHost : envHosts )
            {
                if ( !envHost.getTemplateName().equalsIgnoreCase( ZookeeperClusterConfig.PRODUCT_NAME ) )
                {
                    allowEnv = false;
                    break;
                }
            }
            if ( !allowEnv )
            {
                environments.remove( i-- );
            }
        }

        final BeanContainer<String, Environment> container = new BeanContainer<>( Environment.class );
        container.setBeanIdProperty( "name" );
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
        envList.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                BeanItem<Environment> clusterConfig = container.getItem( valueChangeEvent.getProperty().getValue() );
                Environment environment = clusterConfig.getBean();
                wizard.getConfig().setEnvironmentId( environment.getId() );

                fillConfigServers( zookeeperEnvHostsSelection, environment.getContainerHosts(),
                        zookeeperContainerHosts );
            }
        } );

        return envList;
    }


    private void fillConfigServers( TwinColSelect twinColSelect, Set<ContainerHost> containerHosts,
                                    final Set<UUID> containerHostIds )
    {
        List<ContainerHost> environmentHosts = new ArrayList<>();
        for ( final ContainerHost containerHost : containerHosts )
        {
            if ( !containerHostIds.contains( containerHost.getId() ) )
            {
                environmentHosts.add( containerHost );
            }
        }
        twinColSelect.setValue( null );
        twinColSelect.setContainerDataSource( new BeanItemContainer<>( ContainerHost.class, environmentHosts ) );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
