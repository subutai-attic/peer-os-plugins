package io.subutai.plugin.storm.ui.manager;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.CompleteEvent;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.api.StormNodeOperationTask;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.server.ui.component.ConfirmationDialog;
import io.subutai.server.ui.component.ProgressWindow;
import io.subutai.server.ui.component.TerminalWindow;
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
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION = "AVAILABLE_OPERATIONS";
    protected static final String REFRESH_CLUSTERS_CAPTION = "Refresh Clusters";
    protected static final String CHECK_ALL_BUTTON_CAPTION = "Check All";
    protected static final String CHECK_BUTTON_CAPTION = "Check";
    protected static final String START_ALL_BUTTON_CAPTION = "Start All";
    protected static final String START_BUTTON_CAPTION = "Start";
    protected static final String STOP_ALL_BUTTON_CAPTION = "Stop All";
    protected static final String STOP_BUTTON_CAPTION = "Stop";
    protected static final String REMOVE_CLUSTER = "Remove Cluster";
    protected static final String DESTROY_NODE_BUTTON_CAPTION = "Destroy";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String NODE_ROLE_COLUMN_CAPTION = "Node Role";
    protected static final String STATUS_COLUMN_CAPTION = "Status";
    protected static final String ADD_NODE_BUTTON_CAPTION = "Add Node";
    protected static final String BUTTON_STYLE_NAME = "default";
    private static final Logger LOGGER = LoggerFactory.getLogger( Manager.class );
    private static final String AUTO_SCALE_BUTTON_CAPTION = "Auto Scale";
    private static final String MESSAGE = "No cluster is installed !";
    final Button refreshClustersBtn, checkAllBtn, startAllBtn, stopAllBtn, removeCluster, addNodeBtn;
    private final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    private final GridLayout contentRoot;
    private final ComboBox clusterCombo;
    private final Table masterTable, workersTable;
    private final Storm storm;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private StormClusterConfiguration config;
    private Zookeeper zookeeper;
    private CheckBox autoScaleBtn;


    public Manager( final ExecutorService executorService, final Storm storm, Zookeeper zookeeper,
                    final Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {
        this.executorService = executorService;
        this.storm = storm;
        this.zookeeper = zookeeper;
        this.tracker = tracker;
        this.environmentManager = environmentManager;


        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 11 );
        contentRoot.setColumns( 1 );

        //tables go here
        masterTable = createTableTemplate( "Master node", true );
        workersTable = createTableTemplate( "Workers", false );
        masterTable.setId( "StormMngMasterNode" );
        workersTable.setId( "StormMngWorkerNodes" );
        //tables go here

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );

        clusterCombo = new ComboBox();
        clusterCombo.setId( "StormMngClusterCombo" );
        clusterCombo.setImmediate( true );
        clusterCombo.setNullSelectionAllowed( false );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                config = ( StormClusterConfiguration ) event.getProperty().getValue();
                refreshUI();
                checkAllNodes();
            }
        } );
        controlsContent.addComponent( clusterCombo );
        controlsContent.setComponentAlignment( clusterCombo, Alignment.MIDDLE_CENTER );


        /** Refresh Cluster button */
        refreshClustersBtn = new Button( "Refresh clusters" );
        refreshClustersBtn.setId( "StormMngRefresh" );
        refreshClustersBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                refreshClustersInfo();
            }
        } );
        controlsContent.addComponent( refreshClustersBtn );
        controlsContent.setComponentAlignment( refreshClustersBtn, Alignment.MIDDLE_CENTER );

        /** Check all button */
        checkAllBtn = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAllBtn.setId( "StormCheckAllBtn" );
        addClickListener( checkAllBtn );
        controlsContent.addComponent( checkAllBtn );
        controlsContent.setComponentAlignment( checkAllBtn, Alignment.MIDDLE_CENTER );

        /** Start all button */
        startAllBtn = new Button( START_ALL_BUTTON_CAPTION );
        startAllBtn.setId( "StormStartAllBtn" );
        addClickListener( startAllBtn );
        controlsContent.addComponent( startAllBtn );
        controlsContent.setComponentAlignment( startAllBtn, Alignment.MIDDLE_CENTER );

        /** Stop all button */
        stopAllBtn = new Button( STOP_ALL_BUTTON_CAPTION );
        stopAllBtn.setId( "StormStopAllBtn" );
        addClickListener( stopAllBtn );
        controlsContent.addComponent( stopAllBtn );
        controlsContent.setComponentAlignment( stopAllBtn, Alignment.MIDDLE_CENTER );


        /** Remove Cluster button */
        removeCluster = new Button( REMOVE_CLUSTER );
        removeCluster.setId( "StormRemoveClusterBtn" );
        removeCluster.setDescription( "Removes cluster info from DB" );
        addClickListenerToRemoveClusterButton();
        controlsContent.addComponent( removeCluster );
        controlsContent.setComponentAlignment( removeCluster, Alignment.MIDDLE_CENTER );

        /** Add Node button */
        addNodeBtn = new Button( ADD_NODE_BUTTON_CAPTION );
        addNodeBtn.setId( "StormMngAddNode" );
        addNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( config == null )
                {
                    show( "Select cluster" );
                    return;
                }
                ConfirmationDialog alert = new ConfirmationDialog(
                        String.format( "Do you want to add a new node to the %s cluster?", config.getClusterName() ),
                        "Yes", "No" );
                alert.getOk().addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent clickEvent )
                    {
                        UUID trackId = storm.addNode( config.getClusterName() );
                        ProgressWindow pw = new ProgressWindow( executorService, tracker, trackId,
                                StormClusterConfiguration.PRODUCT_NAME );
                        pw.getWindow().addCloseListener( new Window.CloseListener()
                        {
                            @Override
                            public void windowClose( Window.CloseEvent e )
                            {
                                refreshClustersInfo();
                                refreshUI();
                                checkAllNodes();
                            }
                        } );
                        contentRoot.getUI().addWindow( pw.getWindow() );
                    }
                } );
                contentRoot.getUI().addWindow( alert.getAlert() );
            }
        } );
        controlsContent.addComponent( addNodeBtn );
        controlsContent.setComponentAlignment( addNodeBtn, Alignment.MIDDLE_CENTER );

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
                if ( config == null )
                {
                    show( "Select cluster" );
                }
                else
                {
                    boolean value = ( Boolean ) event.getProperty().getValue();
                    config.setAutoScaling( value );
                    try
                    {
                        storm.saveConfig( config );
                    }
                    catch ( ClusterException e )
                    {
                        show( e.getMessage() );
                    }
                }
            }
        } );


        addStyleNameToButtons( addNodeBtn, removeCluster, startAllBtn, stopAllBtn, checkAllBtn, refreshClustersBtn );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );
        controlsContent.setComponentAlignment( PROGRESS_ICON, Alignment.TOP_CENTER );

        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( masterTable, 0, 1, 0, 5 );
        contentRoot.addComponent( workersTable, 0, 6, 0, 10 );
    }


    private void addClickListenerToRemoveClusterButton()
    {
        removeCluster.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to remove the %s cluster?", config.getClusterName() ), "Yes",
                            "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID track = storm.removeCluster( config.getClusterName() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, track,
                                    StormClusterConfiguration.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
                                    refreshUI();
                                    checkAllNodes();
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
    }


    private void addClickListener( Button button )
    {
        if ( button.getCaption().equals( REFRESH_CLUSTERS_CAPTION ) )
        {
            button.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( final Button.ClickEvent event )
                {
                    refreshClustersInfo();
                }
            } );
            return;
        }
        switch ( button.getCaption() )
        {
            case CHECK_ALL_BUTTON_CAPTION:
                button.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( final Button.ClickEvent event )
                    {
                        if ( config == null )
                        {
                            show( MESSAGE );
                        }
                        else
                        {
                            checkAllNodes();
                        }
                    }
                } );
                break;

            case START_ALL_BUTTON_CAPTION:
                button.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( final Button.ClickEvent event )
                    {
                        if ( config == null )
                        {
                            show( MESSAGE );
                        }
                        else
                        {
                            startAllNodes();
                        }
                    }
                } );
                break;
            case STOP_ALL_BUTTON_CAPTION:
                button.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( final Button.ClickEvent event )
                    {
                        if ( config == null )
                        {
                            show( MESSAGE );
                        }
                        else
                        {
                            stopAllNodes();
                        }
                    }
                } );
                break;
        }
    }


    private void disableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( false );
        }
    }


    private Table createTableTemplate( String caption, boolean master )
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
                    String containerName =
                            ( String ) table.getItem( event.getItemId() ).getItemProperty( "Host" ).getValue();
                    Environment environment = null;
                    try
                    {
                        environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Environment not found.", e );
                        return;
                    }

                    ContainerHost containerHost = null;
                    try
                    {
                        containerHost = environment.getContainerHostByHostname( containerName );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        LOGGER.error( "Container host not found.", e );
                        return;
                    }

                    // Check if the node is involved inside Zookeeper cluster
                    if ( containerHost == null )
                    {
                        ZookeeperClusterConfig zookeeperCluster =
                                zookeeper.getCluster( config.getZookeeperClusterName() );
                        if ( zookeeperCluster != null )
                        {
                            Environment zookeeperEnvironment = null;
                            try
                            {
                                zookeeperEnvironment =
                                        environmentManager.findEnvironment( zookeeperCluster.getEnvironmentId() );
                            }
                            catch ( EnvironmentNotFoundException e )
                            {
                                LOGGER.error( "Environment not found.", e );
                                return;
                            }
                            try
                            {
                                containerHost = zookeeperEnvironment.getContainerHostById( config.getNimbus() );
                            }
                            catch ( ContainerHostNotFoundException e )
                            {
                                LOGGER.error( "Container host not found.", e );
                                return;
                            }
                        }
                    }

                    if ( containerHost != null )
                    {
                        TerminalWindow terminal = new TerminalWindow( containerHost );
                        contentRoot.getUI().addWindow( terminal.getWindow() );
                    }
                    else
                    {
                        show( "Host not found" );
                    }
                }
            }
        } );
        return table;
    }


    public void checkAllNodes()
    {
        batchOperationButton( masterTable, CHECK_BUTTON_CAPTION );
        batchOperationButton( workersTable, CHECK_BUTTON_CAPTION );
    }


    public void startAllNodes()
    {
        batchOperationButton( masterTable, START_BUTTON_CAPTION );
        batchOperationButton( workersTable, START_BUTTON_CAPTION );
    }


    public void stopAllNodes()
    {
        batchOperationButton( masterTable, STOP_BUTTON_CAPTION );
        batchOperationButton( workersTable, STOP_BUTTON_CAPTION );
    }


    private void batchOperationButton( final Table nodesTable, String buttonCaption )
    {
        if ( nodesTable != null )
        {
            for ( Object o : nodesTable.getItemIds() )
            {
                int rowId = ( Integer ) o;
                Item row = nodesTable.getItem( rowId );
                HorizontalLayout availableOperationsLayout =
                        ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
                if ( availableOperationsLayout != null )
                {
                    Button checkBtn = getButtonFromLayout( availableOperationsLayout, buttonCaption );
                    if ( checkBtn != null )
                    {
                        checkBtn.click();
                    }
                }
            }
        }
    }


    private Button getButtonFromLayout( final HorizontalLayout availableOperationsLayout, String caption )
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


    private void show( String notification )
    {
        Notification.show( notification );
    }


    private void refreshUI()
    {
        if ( config != null )
        {
            Environment environment = null;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment not found.", e );
                return;
            }
            Set<ContainerHost> nimbusHost = new HashSet<>();
            if ( !config.isExternalZookeeper() )
            {
                try
                {
                    nimbusHost.add( environment.getContainerHostById( config.getNimbus() ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container host not found.", e );
                    return;
                }
            }
            else
            {
                ZookeeperClusterConfig zookeeperCluster = zookeeper.getCluster( config.getZookeeperClusterName() );
                Environment zookeeperEnvironment = null;
                try
                {
                    zookeeperEnvironment = environmentManager.findEnvironment( zookeeperCluster.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    LOGGER.error( "Environment not found.", e );
                    return;
                }
                try
                {
                    nimbusHost.add( zookeeperEnvironment.getContainerHostById( config.getNimbus() ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container host not found.", e );
                    return;
                }
            }
            populateTable( masterTable, true, nimbusHost );

            Set<ContainerHost> supervisorHosts = new HashSet<>();
            for ( UUID uuid : config.getSupervisors() )
            {
                try
                {
                    supervisorHosts.add( environment.getContainerHostById( uuid ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container host not found.", e );
                    return;
                }
            }
            populateTable( workersTable, false, supervisorHosts );
            autoScaleBtn.setValue( config.isAutoScaling() );
        }
        else
        {
            masterTable.removeAllItems();
            workersTable.removeAllItems();
        }
    }


    private void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    private void populateTable( final Table table, boolean server, Set<ContainerHost> containerHosts )
    {

        table.removeAllItems();

        for ( final ContainerHost containerHost : containerHosts )
        {
            final Label resultHolder = new Label();
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            final Button startBtn = new Button( START_BUTTON_CAPTION );
            final Button stopBtn = new Button( STOP_BUTTON_CAPTION );
            final Button destroyBtn = new Button( DESTROY_NODE_BUTTON_CAPTION );

            checkBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-stormCheck" );
            startBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-stormStart" );
            stopBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-stormStop" );
            destroyBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-stormDestroy" );

            addStyleNameToButtons( checkBtn, startBtn, stopBtn, destroyBtn );

            //            disableButtons( startBtn, stopBtn, restartBtn );
            PROGRESS_ICON.setVisible( false );

            final HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.addStyleName( "default" );
            availableOperations.setSpacing( true );

            if ( server )
            {
                addGivenComponents( availableOperations, checkBtn, startBtn, stopBtn );
                table.addItem( new Object[] {
                        containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ), "Nimbus",
                        resultHolder, availableOperations
                }, null );
            }
            else
            {
                addGivenComponents( availableOperations, checkBtn, startBtn, stopBtn, destroyBtn );
                table.addItem( new Object[] {
                        containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ), "Supervisor",
                        resultHolder, availableOperations
                }, null );
            }


            addClickListenerToCheckButton( containerHost, resultHolder, checkBtn, startBtn, stopBtn, destroyBtn );
            addClickListenerToStartButton( containerHost, checkBtn, startBtn, stopBtn, destroyBtn );
            addClickListenerToStopButton( containerHost, checkBtn, startBtn, stopBtn, destroyBtn );
            if ( !server )
            {
                addClickListenerToDestroyButton( containerHost, checkBtn, startBtn, stopBtn, destroyBtn );
            }
        }
    }


    private void addClickListenerToDestroyButton( final ContainerHost containerHost, final Button... buttons )
    {
        getButton( DESTROY_NODE_BUTTON_CAPTION, buttons ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy %s node ?", containerHost.getHostname() ), "Yes",
                            "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID track = storm.destroyNode( config.getClusterName(), containerHost.getHostname() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, track,
                                    StormClusterConfiguration.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( Window.CloseEvent closeEvent )
                                {
                                    refreshClustersInfo();
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
    }


    private void addClickListenerToStopButton( final ContainerHost containerHost, final Button... buttons )
    {
        getButton( STOP_BUTTON_CAPTION, buttons ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                disableButtons( buttons );
                executorService.execute(
                        new StormNodeOperationTask( storm, tracker, config.getClusterName(), containerHost,
                                NodeOperationType.STOP, new CompleteEvent()
                        {
                            @Override
                            public void onComplete( NodeState nodeState )
                            {
                                synchronized ( PROGRESS_ICON )
                                {
                                    getButton( CHECK_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    getButton( CHECK_BUTTON_CAPTION, buttons ).click();
                                }
                            }
                        }, null ) );
            }
        } );
    }


    private void addClickListenerToStartButton( final ContainerHost containerHost, final Button... buttons )
    {
        getButton( START_BUTTON_CAPTION, buttons ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                disableButtons( buttons );
                executorService.execute(
                        new StormNodeOperationTask( storm, tracker, config.getClusterName(), containerHost,
                                NodeOperationType.START, new CompleteEvent()
                        {
                            @Override
                            public void onComplete( NodeState nodeState )
                            {
                                synchronized ( PROGRESS_ICON )
                                {
                                    getButton( CHECK_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    getButton( CHECK_BUTTON_CAPTION, buttons ).click();
                                }
                            }
                        }, null ) );
            }
        } );
    }


    private void addClickListenerToCheckButton( final ContainerHost containerHost, final Label resultHolder,
                                                final Button... buttons )
    {
        getButton( CHECK_BUTTON_CAPTION, buttons ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                PROGRESS_ICON.setVisible( true );
                resultHolder.setValue( "" );
                disableButtons( buttons );
                executorService.execute(
                        new StormNodeOperationTask( storm, tracker, config.getClusterName(), containerHost,
                                NodeOperationType.STATUS, new CompleteEvent()
                        {
                            @Override
                            public void onComplete( NodeState nodeState )
                            {
                                synchronized ( PROGRESS_ICON )
                                {
                                    if ( nodeState.equals( NodeState.RUNNING ) )
                                    {
                                        getButton( START_BUTTON_CAPTION, buttons ).setEnabled( false );
                                        getButton( STOP_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    }
                                    else if ( nodeState.equals( NodeState.STOPPED ) )
                                    {
                                        getButton( START_BUTTON_CAPTION, buttons ).setEnabled( true );
                                        getButton( STOP_BUTTON_CAPTION, buttons ).setEnabled( false );
                                    }
                                    else if ( nodeState.equals( NodeState.UNKNOWN ) )
                                    {
                                        getButton( START_BUTTON_CAPTION, buttons ).setEnabled( true );
                                        getButton( STOP_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    }
                                    resultHolder.setValue( nodeState.name() );
                                    PROGRESS_ICON.setVisible( false );
                                    getButton( CHECK_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    if ( getButton( DESTROY_NODE_BUTTON_CAPTION, buttons ) != null )
                                    {
                                        getButton( DESTROY_NODE_BUTTON_CAPTION, buttons ).setEnabled( true );
                                    }
                                }
                            }
                        }, null ) );
            }
        } );
    }


    private Button getButton( String caption, Button... buttons )
    {
        for ( Button b : buttons )
        {
            if ( b.getCaption().equals( caption ) )
            {
                return b;
            }
        }
        return null;
    }


    private void addGivenComponents( HorizontalLayout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
        }
    }


    public void refreshClustersInfo()
    {
        StormClusterConfiguration current = ( StormClusterConfiguration ) clusterCombo.getValue();
        clusterCombo.removeAllItems();
        List<StormClusterConfiguration> clustersInfo = storm.getClusters();
        if ( clustersInfo != null && !clustersInfo.isEmpty() )
        {
            for ( StormClusterConfiguration ci : clustersInfo )
            {
                clusterCombo.addItem( ci );
                clusterCombo.setItemCaption( ci, ci.getClusterName() );
            }
            if ( current != null )
            {
                for ( StormClusterConfiguration ci : clustersInfo )
                {
                    if ( ci.getClusterName().equals( current.getClusterName() ) )
                    {
                        clusterCombo.setValue( ci );
                        return;
                    }
                }
            }
            else
            {
                for ( StormClusterConfiguration ci : clustersInfo )
                {
                    if ( ci.getNimbus() != null )
                    {
                        clusterCombo.setValue( ci );
                        return;
                    }
                }
            }
        }
    }

    public Component getContent()
    {
        return contentRoot;
    }
}
