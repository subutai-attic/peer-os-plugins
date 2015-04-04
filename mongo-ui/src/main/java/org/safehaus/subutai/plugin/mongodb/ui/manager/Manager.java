/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.ui.manager;


import java.util.*;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterOperationTask;
import org.safehaus.subutai.plugin.mongodb.api.MongoNodeOperationTask;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.server.ui.component.ConfirmationDialog;
import org.safehaus.subutai.server.ui.component.ProgressWindow;
import org.safehaus.subutai.server.ui.component.TerminalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.event.ItemClickEvent;
import com.vaadin.server.Sizeable;
import com.vaadin.server.ThemeResource;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Table;
import com.vaadin.ui.Window;


public class Manager
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Manager.class );
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION = "AVAILABLE_OPERATIONS";
    protected static final String REFRESH_CLUSTERS_CAPTION = "Refresh Clusters";
    protected static final String CHECK_ALL_BUTTON_CAPTION = "Check All";
    protected static final String CHECK_BUTTON_CAPTION = "Check";
    protected static final String START_ALL_BUTTON_CAPTION = "Start All";
    protected static final String START_BUTTON_CAPTION = "Start";
    protected static final String STOP_ALL_BUTTON_CAPTION = "Stop All";
    protected static final String STOP_BUTTON_CAPTION = "Stop";
    protected static final String DESTROY_BUTTON_CAPTION = "Destroy";
    protected static final String DESTROY_CLUSTER_BUTTON_CAPTION = "Destroy Cluster";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String NODE_ROLE_COLUMN_CAPTION = "Node Role";
    protected static final String STATUS_COLUMN_CAPTION = "Status";
    protected static final String BUTTON_STYLE_NAME = "default";
    private static final String AUTO_SCALE_BUTTON_CAPTION = "Auto Scale";

    final Button refreshClustersBtn, startAllBtn, stopAllBtn, checkAllBtn, destroyClusterBtn;
    private final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    private final GridLayout contentRoot;
    private final ComboBox clusterCombo;
    private final Table configServersTable;
    private final Table routersTable;
    private final Table dataNodesTable;
    private final Label replicaSetName;
    private final Label domainName;
    private final Label cfgSrvPort;
    private final Label routerPort;
    private final Label dataNodePort;
    private CheckBox autoScaleBtn;

    private final ExecutorService executorService;
    private final Tracker tracker;
    private final Mongo mongo;
    private final EnvironmentManager environmentManager;
    private MongoClusterConfig mongoClusterConfig;


    public Manager( final ExecutorService executorService, final Mongo mongo,
                    final EnvironmentManager environmentManager, final Tracker tracker ) throws NamingException
    {

        this.executorService = executorService;
        this.mongo = mongo;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 11 );
        contentRoot.setColumns( 1 );

        //tables go here
        configServersTable = createTableTemplate( "Config Servers" );
        configServersTable.setId( "MongoConfigsserversTbl" );
        routersTable = createTableTemplate( "Query Routers" );
        routersTable.setId( "MongoRoutersTbl" );
        dataNodesTable = createTableTemplate( "Data Nodes" );
        dataNodesTable.setId( "MongoDataNodesTbl" );

        final HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        final Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );
        controlsContent.setComponentAlignment( clusterNameLabel, Alignment.MIDDLE_CENTER );

        clusterCombo = new ComboBox();
        clusterCombo.setId( "MongoClusterCb" );
        clusterCombo.setImmediate( true );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                mongoClusterConfig = ( MongoClusterConfig ) event.getProperty().getValue();
                refreshUI();
            }
        } );
        controlsContent.addComponent( clusterCombo );
        controlsContent.setComponentAlignment( clusterCombo, Alignment.MIDDLE_CENTER );

        refreshClustersBtn = new Button( REFRESH_CLUSTERS_CAPTION );
        refreshClustersBtn.setId( "MongoRefreshClustersBtn" );
        refreshClustersBtn.addStyleName( "default" );
        refreshClustersBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                refreshClustersInfo();
                checkAllBtn.click();
            }
        } );

        controlsContent.addComponent( refreshClustersBtn );
        controlsContent.setComponentAlignment( refreshClustersBtn, Alignment.MIDDLE_CENTER );

        checkAllBtn = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAllBtn.setId( "MongoCheckAllBtn" );
        checkAllBtn.addStyleName( "default" );
        checkAllBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                checkNodesStatus( configServersTable );
                checkNodesStatus( routersTable );
                checkNodesStatus( dataNodesTable );
            }
        } );
        controlsContent.addComponent( checkAllBtn );
        controlsContent.setComponentAlignment( checkAllBtn, Alignment.MIDDLE_CENTER );

        startAllBtn = new Button( START_ALL_BUTTON_CAPTION );
        startAllBtn.setId( "MongoStartAllBtn" );
        startAllBtn.addStyleName( "default" );
        startAllBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                executorService.execute( new MongoClusterOperationTask( mongo, tracker, mongoClusterConfig.getClusterName(),
                        ClusterOperationType.START_ALL, new CompleteEvent()
                {
                    @Override
                    public void onComplete( final NodeState state )
                    {
                        checkAllBtn.click();
                    }
                }, environmentManager, null ) );
            }
        } );
        controlsContent.addComponent( startAllBtn );
        controlsContent.setComponentAlignment( startAllBtn, Alignment.MIDDLE_CENTER );

        stopAllBtn = new Button( STOP_ALL_BUTTON_CAPTION );
        stopAllBtn.setId( "MongoStopAllBtn" );
        stopAllBtn.addStyleName( "default" );
        stopAllBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                stopAllNodes( configServersTable );
                stopAllNodes( routersTable );
                stopAllNodes( dataNodesTable );
            }
        } );
        controlsContent.addComponent( stopAllBtn );
        controlsContent.setComponentAlignment( stopAllBtn, Alignment.MIDDLE_CENTER );

        destroyClusterBtn = new Button( DESTROY_CLUSTER_BUTTON_CAPTION );
        destroyClusterBtn.setId( "MongoDestroyClusterBtn" );
        destroyClusterBtn.addStyleName( "default" );
        destroyClusterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( mongoClusterConfig != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s cluster?",
                                    mongoClusterConfig.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID trackID = mongo.uninstallCluster( mongoClusterConfig.getClusterName() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    MongoClusterConfig.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
                                    checkAllBtn.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );

                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
                else
                {
                    show( "Please, select cluster" );
                }
            }
        } );

        controlsContent.addComponent( destroyClusterBtn );
        controlsContent.setComponentAlignment( destroyClusterBtn, Alignment.MIDDLE_CENTER );

        Button addRouterBtn = new Button( "Add Router" );
        addRouterBtn.setId( "MongoAddRouterBtn" );
        addRouterBtn.addStyleName( "default" );
        addRouterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( mongoClusterConfig != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to add ROUTER to the %s cluster?",
                                    mongoClusterConfig.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            //TODO add comboBox representing available peers and nodes
                            UUID trackID = mongo.addNode( mongoClusterConfig.getClusterName(), NodeType.ROUTER_NODE );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    MongoClusterConfig.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
                                    checkAllBtn.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );

                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
                else
                {
                    show( "Please, select cluster" );
                }
            }
        } );

        Button addDataNodeBtn = new Button( "Add Data Node" );
        addDataNodeBtn.setId( "MongoAddNodeBtn" );
        addDataNodeBtn.addStyleName( "default" );
        addDataNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( mongoClusterConfig != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to add DATA_NODE to the %s cluster?",
                                    mongoClusterConfig.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            //TODO add comboBox representing available peers
                            UUID trackID = mongo.addNode( mongoClusterConfig.getClusterName(), NodeType.DATA_NODE );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    MongoClusterConfig.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
                                    checkAllBtn.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );

                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
                else
                {
                    show( "Please, select cluster" );
                }
            }
        } );

        controlsContent.addComponent( addRouterBtn );
        controlsContent.addComponent( addDataNodeBtn );
        controlsContent.setComponentAlignment( addRouterBtn, Alignment.MIDDLE_CENTER );
        controlsContent.setComponentAlignment( addDataNodeBtn, Alignment.MIDDLE_CENTER );

        //auto scale button
        autoScaleBtn = new CheckBox( AUTO_SCALE_BUTTON_CAPTION );
        autoScaleBtn.setValue( false );
        autoScaleBtn.addStyleName( BUTTON_STYLE_NAME );
        controlsContent.addComponent( autoScaleBtn );
        controlsContent.setComponentAlignment( autoScaleBtn, Alignment.MIDDLE_CENTER );
        autoScaleBtn.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent event )
            {
                if ( mongoClusterConfig == null )
                {
                    show( "Select cluster" );
                }
                else
                {
                    boolean value = ( Boolean ) event.getProperty().getValue();
                    mongoClusterConfig.setAutoScaling( value );
                    try
                    {
                        mongo.saveConfig( mongoClusterConfig );
                    }
                    catch ( ClusterException e )
                    {
                        show( e.getMessage() );
                    }
                }
            }
        } );

        HorizontalLayout configContent = new HorizontalLayout();
        configContent.setSpacing( true );

        replicaSetName = new Label();
        domainName = new Label();
        cfgSrvPort = new Label();
        routerPort = new Label();
        dataNodePort = new Label();

        configContent.addComponent( new Label( "Replica Set:" ) );
        configContent.addComponent( replicaSetName );
        configContent.addComponent( new Label( "Domain:" ) );
        configContent.addComponent( domainName );
        configContent.addComponent( new Label( "Config server port:" ) );
        configContent.addComponent( cfgSrvPort );
        configContent.addComponent( new Label( "Router port:" ) );
        configContent.addComponent( routerPort );
        configContent.addComponent( new Label( "Data node port:" ) );
        configContent.addComponent( dataNodePort );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );

        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( configContent, 0, 1 );
        contentRoot.addComponent( configServersTable, 0, 2, 0, 4 );
        contentRoot.addComponent( routersTable, 0, 5, 0, 7 );
        contentRoot.addComponent( dataNodesTable, 0, 8, 0, 10 );
    }


    private Table createTableTemplate( String caption )
    {
        final Table table = new Table( caption );
        table.addContainerProperty( HOST_COLUMN_CAPTION, String.class, null );
        table.addContainerProperty( IP_COLUMN_CAPTION, String.class, null );
        table.addContainerProperty( NODE_ROLE_COLUMN_CAPTION, String.class, null );
        table.addContainerProperty( STATUS_COLUMN_CAPTION, Label.class, null );
        table.addContainerProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION, HorizontalLayout.class, null );

        table.setSizeFull();
        table.setPageLength( 10 );
        table.setSelectable( false );
        table.setImmediate( true );

        table.addItemClickListener( new ItemClickEvent.ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                if ( event.isDoubleClick() )
                {
                    String containerId =
                            ( String ) table.getItem( event.getItemId() ).getItemProperty( HOST_COLUMN_CAPTION )
                                            .getValue();
                    Set<ContainerHost> containerHosts = null;
                    try
                    {
                        containerHosts = environmentManager.findEnvironment( mongoClusterConfig.getEnvironmentId() )
                                                           .getContainerHosts();
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Error getting environment.", e );
                        show( "Error getting environment" );
                        return;
                    }

                    Iterator iterator = containerHosts.iterator();
                    ContainerHost containerHost = null;
                    while ( iterator.hasNext() )
                    {
                        containerHost = ( ContainerHost ) iterator.next();
                        if ( containerHost.getId().equals( UUID.fromString( containerId ) ) )
                        {
                            break;
                        }
                    }
                    if ( containerHost != null )
                    {
                        TerminalWindow terminal = new TerminalWindow( containerHost );
                        contentRoot.getUI().addWindow( terminal.getWindow() );
                    }
                    else
                    {
                        show( "Agent is not connected" );
                    }
                }
            }
        } );
        return table;
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }


    private void refreshUI()
    {
        if ( mongoClusterConfig != null )
        {
            Environment environment = null;
            try {
                environment = environmentManager.findEnvironment( mongoClusterConfig.getEnvironmentId() );
            } catch (EnvironmentNotFoundException e) {
                e.printStackTrace();
            }
            assert environment != null;
            Set<ContainerHost> configHosts = new HashSet<>();
            for ( UUID uuid : mongoClusterConfig.getConfigHosts() )
            {
                try {
                    configHosts.add( environment.getContainerHostById( uuid ) );
                } catch (ContainerHostNotFoundException e) {
                    e.printStackTrace();
                }
            }
            Set<ContainerHost> routerHosts = new HashSet<>();
            for ( UUID uuid : mongoClusterConfig.getRouterHosts() )
            {
                try {
                    routerHosts.add( environment.getContainerHostById( uuid ) );
                } catch (ContainerHostNotFoundException e) {
                    e.printStackTrace();
                }
            }

            Set<ContainerHost> dataHosts = new HashSet<>();
            for ( UUID uuid : mongoClusterConfig.getDataHosts() )
            {
                try {
                    dataHosts.add( environment.getContainerHostById( uuid ) );
                } catch (ContainerHostNotFoundException e) {
                    e.printStackTrace();
                }
            }

            populateTable( configServersTable, configHosts, NodeType.CONFIG_NODE );
            populateTable( routersTable, routerHosts, NodeType.ROUTER_NODE );
            populateTable( dataNodesTable, dataHosts, NodeType.DATA_NODE );
            replicaSetName.setValue( mongoClusterConfig.getReplicaSetName() );
            domainName.setValue( mongoClusterConfig.getDomainName() );
            cfgSrvPort.setValue( mongoClusterConfig.getCfgSrvPort() + "" );
            routerPort.setValue( mongoClusterConfig.getRouterPort() + "" );
            dataNodePort.setValue( mongoClusterConfig.getDataNodePort() + "" );
            autoScaleBtn.setValue( mongoClusterConfig.isAutoScaling() );
        }
        else
        {
            configServersTable.removeAllItems();
            routersTable.removeAllItems();
            dataNodesTable.removeAllItems();
            replicaSetName.setValue( "" );
            domainName.setValue( "" );
            cfgSrvPort.setValue( "" );
            routerPort.setValue( "" );
            dataNodePort.setValue( "" );
        }
    }


    private void populateTable( final Table table, Set<ContainerHost> nodes, final NodeType nodeType )
    {
        table.removeAllItems();

        for ( final ContainerHost node : nodes )
        {
            final Label resultHolder = new Label();
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            checkBtn.setId( node.getIpByInterfaceName( "eth0" ) + "-mongoCheck" );
            final Button startBtn = new Button( START_BUTTON_CAPTION );
            startBtn.setId( node.getIpByInterfaceName( "eth0" ) + "-mongoStart" );
            final Button stopBtn = new Button( STOP_BUTTON_CAPTION );
            stopBtn.setId( node.getIpByInterfaceName( "eth0" ) + "-mongoStop" );
            final Button destroyBtn = new Button( DESTROY_BUTTON_CAPTION );
            destroyBtn.setId( node.getIpByInterfaceName( "eth0" ) + "-mongoDestroy" );

            addStyleNameToButtons( checkBtn, startBtn, stopBtn, destroyBtn );

            final HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.addStyleName( "default" );
            availableOperations.setSpacing( true );

            if ( table.getCaption().equals( "Config Servers" ) ){
                addGivenComponents( availableOperations, checkBtn, startBtn, stopBtn );
            }
            else{
                addGivenComponents( availableOperations, checkBtn, startBtn, stopBtn, destroyBtn );
            }

            table.addItem( new Object[] {
                    node.getHostname(), node.getIpByInterfaceName( "eth0" ),
                    nodeType.name(), resultHolder, availableOperations
            }, null );


            checkBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( startBtn, stopBtn, destroyBtn, checkBtn );
                    executorService.execute( new MongoNodeOperationTask( mongo, tracker,
                            mongoClusterConfig.getClusterName(), node, NodeOperationType.STATUS, nodeType,
                            new CompleteEvent()
                            {
                                @Override
                                public void onComplete( NodeState nodeState )
                                {
                                    synchronized ( PROGRESS_ICON )
                                    {
                                        if ( nodeState == NodeState.RUNNING )
                                        {
                                            stopBtn.setEnabled( true );
                                        }
                                        else if ( nodeState == NodeState.STOPPED )
                                        {
                                            startBtn.setEnabled( true );
                                        }
                                        resultHolder.setValue( nodeState.name() );
                                        enableButtons( destroyBtn, checkBtn );
                                        PROGRESS_ICON.setVisible(false);
                                    }
                                }
                            }, null ) );
                }
            } );

            startBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( startBtn, stopBtn, destroyBtn, checkBtn );
                    executorService.execute( new MongoNodeOperationTask( mongo, tracker,
                            mongoClusterConfig.getClusterName(), node, NodeOperationType.START, nodeType,
                            new CompleteEvent()
                            {
                                @Override
                                public void onComplete( NodeState nodeState )
                                {
                                    synchronized ( PROGRESS_ICON )
                                    {
                                        enableButtons( destroyBtn, checkBtn );
                                        checkBtn.click();
                                    }
                                }
                            }, null ) );
                }
            } );

            stopBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( startBtn, stopBtn, destroyBtn, checkBtn );
                    executorService.execute( new MongoNodeOperationTask( mongo, tracker,
                            mongoClusterConfig.getClusterName(), node, NodeOperationType.STOP, nodeType,
                            new CompleteEvent()
                            {
                                @Override
                                public void onComplete( NodeState nodeState )
                                {
                                    synchronized ( PROGRESS_ICON )
                                    {
                                        enableButtons( destroyBtn, checkBtn );
                                        checkBtn.click();
                                    }
                                }
                            }, null ) );
                }
            } );

            destroyBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent clickEvent )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s node?", node.getHostname() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID trackID = mongo.destroyNode( mongoClusterConfig.getClusterName(), node.getHostname(),
                                    nodeType );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    MongoClusterConfig.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
                                    checkAllBtn.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );

                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
            } );
        }
    }


    private void addGivenComponents( HorizontalLayout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
        }
    }


    private void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    public void refreshClustersInfo()
    {
        List<MongoClusterConfig> mongoClusterInfos = mongo.getClusters();
        MongoClusterConfig clusterInfo = ( MongoClusterConfig ) clusterCombo.getValue();
        clusterCombo.removeAllItems();
        if ( mongoClusterInfos != null && !mongoClusterInfos.isEmpty() )
        {
            for ( MongoClusterConfig mongoClusterInfo : mongoClusterInfos )
            {
                clusterCombo.addItem( mongoClusterInfo );
                clusterCombo.setItemCaption( mongoClusterInfo, mongoClusterInfo.getClusterName() );
            }
            if ( clusterInfo != null )
            {
                for ( MongoClusterConfig mongoClusterInfo : mongoClusterInfos )
                {
                    if ( mongoClusterInfo.getClusterName().equals( clusterInfo.getClusterName() ) )
                    {
                        mongoClusterConfig = mongoClusterInfo;
                        clusterCombo.setValue( mongoClusterInfo );
                        return;
                    }
                }
            }
            else
            {
                mongoClusterConfig = mongoClusterInfos.iterator().next();
                clusterCombo.setValue( mongoClusterConfig );
            }
        }
    }


    public void disableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( false );
        }
    }


    public void enableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( true );
        }
    }


    public void checkNodesStatus( Table table )
    {
        for ( Object o : table.getItemIds() )
        {
            int rowId = ( Integer ) o;
            Item row = table.getItem( rowId );
            HorizontalLayout availableOperationsLayout =
                    ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
            if ( availableOperationsLayout != null )
            {
                Button checkBtn = getButton( availableOperationsLayout, CHECK_BUTTON_CAPTION );
                if ( checkBtn != null )
                {
                    checkBtn.click();
                }
            }
        }
    }


    protected Button getButton( final HorizontalLayout availableOperationsLayout, String caption )
    {
        if ( availableOperationsLayout == null )
        {
            return null;
        }
        else
        {
            for ( Component component : availableOperationsLayout )
            {
                if ( component.getCaption().equals( caption ) )
                {
                    return ( Button ) component;
                }
            }
            return null;
        }
    }


    public void stopAllNodes( Table table )
    {
        for ( Object o : table.getItemIds() )
        {
            int rowId = ( Integer ) o;
            Item row = table.getItem( rowId );
            HorizontalLayout availableOperationsLayout =
                    ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
            if ( availableOperationsLayout != null )
            {
                Button stopBtn = getButton( availableOperationsLayout, STOP_BUTTON_CAPTION );
                if ( stopBtn != null )
                {
                    stopBtn.click();
                }
            }
        }
    }


    public void checkAllNodes()
    {
        checkNodesStatus( configServersTable );
        checkNodesStatus( routersTable );
        checkNodesStatus( dataNodesTable );
    }


    public void startAllNodes( Table table )
    {
        for ( Object o : table.getItemIds() )
        {
            int rowId = ( Integer ) o;
            Item row = table.getItem( rowId );
            HorizontalLayout availableOperationsLayout =
                    ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
            if ( availableOperationsLayout != null )
            {
                Button startBtn = getButton( availableOperationsLayout, START_BUTTON_CAPTION );
                if ( startBtn != null )
                {
                    startBtn.click();
                }
            }
        }
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
