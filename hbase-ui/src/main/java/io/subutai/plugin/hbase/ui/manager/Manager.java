/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.hbase.ui.manager;


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
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.common.api.NodeType;
import io.subutai.plugin.common.ui.AddNodeWindow;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.api.HBaseNodeOperationTask;
import io.subutai.server.ui.component.ConfirmationDialog;
import io.subutai.server.ui.component.ProgressWindow;
import io.subutai.server.ui.component.TerminalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
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
    private final static Logger LOGGER = LoggerFactory.getLogger( Manager.class );

    public final static String START_STOP_BUTTON_DEFAULT_CAPTION = "Start/Stop";
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION = "AVAILABLE_OPERATIONS";
    protected static final String REFRESH_CLUSTER_CAPTION = "Refresh Clusters";
    protected static final String CHECK_ALL_BUTTON_CAPTION = "Check All";
    protected static final String CHECK_BUTTON_CAPTION = "Check";
    protected static final String START_ALL_BUTTON_CAPTION = "Start All";
    protected static final String START_BUTTON_CAPTION = "Start";
    protected static final String STOP_ALL_BUTTON_CAPTION = "Stop All";
    protected static final String STOP_BUTTON_CAPTION = "Stop";
    protected static final String REMOVE_CLUSTER_BUTTON_CAPTION = "Remove Cluster";
    protected static final String DESTROY_BUTTON_CAPTION = "Destroy";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String NODE_ROLE_COLUMN_CAPTION = "Node Role";
    protected static final String STATUS_COLUMN_CAPTION = "Status";
    protected static final String ADD_NODE_CAPTION = "Add Node";
    protected static final String TABLE_CAPTION = "All Nodes";
    protected static final String BUTTON_STYLE_NAME = "default";
    private final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );

    private static final String AUTO_SCALE_BUTTON_CAPTION = "Auto Scale";
    protected final Button refreshClustersBtn, startAllNodesBtn, stopAllNodesBtn, checkAllBtn, removeClusterBtn,
            addNodeBtn;
    private final GridLayout contentRoot;
    private final ComboBox clusterCombo;
    private final ExecutorService executor;
    private final HBase hbase;
    private final Hadoop hadoop;
    private final Tracker tracker;
    private final String MESSAGE = "No cluster is installed !";
    private final EnvironmentManager environmentManager;
    private HBaseConfig config = new HBaseConfig();
    private Table nodesTable = null;
    private CheckBox autoScaleBtn;


    public Manager( final ExecutorService executor, final HBase hbase, final Hadoop hadoop, final Tracker tracker,
                    EnvironmentManager environmentManager ) throws NamingException
    {
        Preconditions.checkNotNull( executor, "Executor is null" );

        this.hbase = hbase;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.executor = executor;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 10 );
        contentRoot.setColumns( 1 );

        //tables go here
        nodesTable = createTableTemplate( TABLE_CAPTION );
        contentRoot.setId( "HbaseMngContentRoot" );
        nodesTable.setId( "HbaseMngNodesTable" );


        final HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );
        controlsContent.setComponentAlignment( clusterNameLabel, Alignment.MIDDLE_CENTER );

        clusterCombo = new ComboBox();
        clusterCombo.setId( "HbaseMngClusterCombo" );
        clusterCombo.setImmediate( true );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                Object value = event.getProperty().getValue();
                config = value != null ? ( HBaseConfig ) value : null;
                refreshUI();
            }
        } );
        controlsContent.addComponent( clusterCombo );
        controlsContent.setComponentAlignment( clusterCombo, Alignment.MIDDLE_CENTER );


        /** Refresh Cluster button */
        refreshClustersBtn = new Button( REFRESH_CLUSTER_CAPTION );
        refreshClustersBtn.setId( "HbaseMngRefresh" );
        refreshClustersBtn.addStyleName( BUTTON_STYLE_NAME );
        refreshClustersBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                new Thread( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        refreshClustersInfo();
                    }
                } ).start();
            }
        } );
        controlsContent.addComponent( refreshClustersBtn );
        controlsContent.setComponentAlignment( refreshClustersBtn, Alignment.MIDDLE_CENTER );


        /** Check All button */
        checkAllBtn = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAllBtn.setId( "HbaseMngCheck" );
        checkAllBtn.addStyleName( BUTTON_STYLE_NAME );
        checkAllBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
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
        controlsContent.addComponent( checkAllBtn );
        controlsContent.setComponentAlignment( checkAllBtn, Alignment.MIDDLE_CENTER );


        /** Start All button */
        startAllNodesBtn = new Button( START_ALL_BUTTON_CAPTION );
        startAllNodesBtn.setId( "HbaseMngStart" );
        startAllNodesBtn.addStyleName( BUTTON_STYLE_NAME );
        startAllNodesBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
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
        controlsContent.addComponent( startAllNodesBtn );
        controlsContent.setComponentAlignment( startAllNodesBtn, Alignment.MIDDLE_CENTER );


        /** Stop All button */
        stopAllNodesBtn = new Button( STOP_ALL_BUTTON_CAPTION );
        stopAllNodesBtn.setId( "HbaseMngStop" );
        stopAllNodesBtn.addStyleName( BUTTON_STYLE_NAME );
        stopAllNodesBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
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
        controlsContent.addComponent( stopAllNodesBtn );
        controlsContent.setComponentAlignment( stopAllNodesBtn, Alignment.MIDDLE_CENTER );


        /** Add Node button */
        addNodeBtn = new Button( ADD_NODE_CAPTION );
        addNodeBtn.setId( "HbaseMngAddNode" );
        addNodeBtn.addStyleName( BUTTON_STYLE_NAME );
        addClickListenerToAddNodeButton();
        controlsContent.addComponent( addNodeBtn );
        controlsContent.setComponentAlignment( addNodeBtn, Alignment.MIDDLE_CENTER );


        /** Destroy All button */
        removeClusterBtn = new Button( REMOVE_CLUSTER_BUTTON_CAPTION );
        removeClusterBtn.setId( "HbaseMngDestroy" );
        removeClusterBtn.addStyleName( BUTTON_STYLE_NAME );
        removeClusterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to add node to the %s cluster?", config.getClusterName() ),
                            "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID trackID = hbase.uninstallCluster( config.getClusterName() );
                            ProgressWindow window =
                                    new ProgressWindow( executor, tracker, trackID, HBaseConfig.PRODUCT_KEY );
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

        controlsContent.addComponent( removeClusterBtn );
        controlsContent.setComponentAlignment( removeClusterBtn, Alignment.MIDDLE_CENTER );

        //auto scale button
        autoScaleBtn = new CheckBox( AUTO_SCALE_BUTTON_CAPTION );
        autoScaleBtn.setValue( false );
        autoScaleBtn.addStyleName( "default" );
        controlsContent.addComponent( autoScaleBtn );
        autoScaleBtn.setValue( config.isAutoScaling() );
        controlsContent.setComponentAlignment( autoScaleBtn, Alignment.MIDDLE_CENTER );
        autoScaleBtn.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent event )
            {
                if ( config.getClusterName() == null )
                {
                    show( "Select cluster" );
                }
                else
                {
                    boolean value = ( Boolean ) event.getProperty().getValue();
                    config.setAutoScaling( value );
                    try
                    {
                        hbase.saveConfig( config );
                    }
                    catch ( ClusterException e )
                    {
                        show( e.getMessage() );
                    }
                }
            }
        } );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );

        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( nodesTable, 0, 1, 0, 9 );
    }


    public void checkAllNodes()
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
                    Button checkBtn = getButton( availableOperationsLayout, CHECK_BUTTON_CAPTION );
                    if ( checkBtn != null )
                    {
                        checkBtn.click();
                    }
                }
            }
        }
    }


    public void addClickListenerToAddNodeButton()
    {
        addNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                if ( config == null )
                {
                    show( "Select cluster" );
                    return;
                }

                Environment environment;
                try
                {
                    environment = environmentManager
                            .findEnvironment( hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    LOGGER.error( "Error getting environment by id: " + config.getEnvironmentId().toString(), e );
                    return;
                }

                Set<ContainerHost> set = null;

                String hn = config.getHadoopClusterName();
                if ( !Strings.isNullOrEmpty( hn ) )
                {
                    HadoopClusterConfig hci = hadoop.getCluster( hn );
                    if ( hci != null )
                    {
                        try
                        {
                            set = environment.getContainerHostsByIds( Sets.newHashSet( hci.getAllNodes() ) );
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            LOGGER.error( "Container hosts not found by ids: " + hci.getAllNodes().toString(), e );
                        }
                    }
                }

                if ( set == null )
                {
                    show( "Hadoop cluster not found" );
                    return;
                }

                try
                {
                    set.removeAll( environment.getContainerHostsByIds( Sets.newHashSet( config.getRegionServers() ) ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOGGER.error( "Container hosts not found by ids: " + config.getAllNodes().toString(), e );
                }
                if ( set.isEmpty() )
                {
                    show( "There is no node left to add as HRegionServer." );
                    return;
                }

                AddNodeWindow w = new AddNodeWindow( hbase, executor, tracker, config, set );
                contentRoot.getUI().addWindow( w );
                w.addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
                        refreshClustersInfo();
                    }
                } );
            }
        } );
    }


    private void stopAllNodes()
    {
        PROGRESS_ICON.setVisible( true );
        disableOREnableAllButtonsOnTable( nodesTable, false );
        executor.execute( new StopTask( hbase, tracker, config.getClusterName(), new CompleteEvent()
        {
            @Override
            public void onComplete( String result )
            {
                synchronized ( PROGRESS_ICON )
                {
                    disableOREnableAllButtonsOnTable( nodesTable, true );
                    checkAllNodes();
                }
            }
        } ) );
    }


    private void startAllNodes()
    {
        PROGRESS_ICON.setVisible( true );
        disableOREnableAllButtonsOnTable( nodesTable, false );
        executor.execute( new StartTask( hbase, tracker, config.getClusterName(), new CompleteEvent()
        {
            @Override
            public void onComplete( String result )
            {
                synchronized ( PROGRESS_ICON )
                {
                    disableOREnableAllButtonsOnTable( nodesTable, true );
                    checkAllNodes();
                }
            }
        } ) );
    }


    private void disableOREnableAllButtonsOnTable( Table table, boolean value )
    {
        if ( table != null )
        {
            for ( Object o : table.getItemIds() )
            {
                int rowId = ( Integer ) o;
                Item row = table.getItem( rowId );
                HorizontalLayout availableOperationsLayout =
                        ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
                if ( availableOperationsLayout != null )
                {
                    for ( Component component : availableOperationsLayout )
                    {
                        component.setEnabled( value );
                    }
                }
            }
        }
    }


    private void populateTable( final Table table, Set<UUID> containerHosts )
    {
        for ( final UUID containerHost : containerHosts )
        {
            Environment environment;
            try
            {
                environment = environmentManager
                        .findEnvironment( hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() );
                final ContainerHost host = environment.getContainerHostById( containerHost );

                final List<NodeType> roles = config.getNodeRoles( host );
                for ( final NodeType role : roles )
                {
                    final Label resultHolder = new Label();
                    final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
                    checkBtn.setId( host.getIpByInterfaceName( "eth0" ) + "-hbaseCheck" );
                    checkBtn.addStyleName( BUTTON_STYLE_NAME );
                    final Button destroyBtn = new Button( DESTROY_BUTTON_CAPTION );
                    destroyBtn.addStyleName( BUTTON_STYLE_NAME );
                    final Button startNStopButton = new Button( START_STOP_BUTTON_DEFAULT_CAPTION );
                    startNStopButton.addStyleName( BUTTON_STYLE_NAME );

                    PROGRESS_ICON.setVisible( false );

                    final HorizontalLayout availableOperations = new HorizontalLayout();
                    availableOperations.addStyleName( BUTTON_STYLE_NAME );
                    availableOperations.setSpacing( true );

                    availableOperations.addComponent( checkBtn );
                    if ( role.equals( NodeType.HREGIONSERVER ) )
                    {

                        /*
                        TODO: We cannot start region servers from UI via calling "hbase-daemon.sh start regionservers"
                        TODO: command, however this command works properly when it is called from linux terminal.
                        TODO: Currently I could not find a solution for this, if we can find we should enable below
                        TODO: code block.
                        availableOperations.addComponent( startNStopButton );
                        startNStopButton.addClickListener( new Button.ClickListener()
                        {
                            @Override
                            public void buttonClick( final Button.ClickEvent clickEvent )
                            {
                                if ( startNStopButton.getCaption().equals( START_BUTTON_CAPTION ) )
                                {

                                    String status = getHmasterNodeState().getValue();
                                    if ( Objects.equals( status, NodeState.STOPPED.name() ) ){
                                        show( "Please start " + NodeType.HMASTER + " and " + NodeType.HQUORUMPEER + "
                                         first.!!!" );
                                        return;
                                    }

                                    PROGRESS_ICON.setVisible( true );
                                    checkBtn.setEnabled( false );
                                    startNStopButton.setEnabled( false );
                                    destroyBtn.setEnabled( false );

                                    executor.execute(
                                            new HBaseNodeOperationTask( hbase, tracker, config.getClusterName(), host,
                                                    NodeOperationType.START, role,
                                                    new io.subutai.plugin.common.api.CompleteEvent()
                                                    {
                                                        @Override
                                                        public void onComplete( final NodeState state )
                                                        {
                                                            synchronized ( PROGRESS_ICON )
                                                            {
                                                                PROGRESS_ICON.setVisible( false );
                                                                checkBtn.setEnabled( true );
                                                                checkBtn.click();
                                                            }
                                                        }
                                                    }, null ) );
                                }
                                else if ( startNStopButton.getCaption().equals( STOP_BUTTON_CAPTION ) )
                                {

                                    PROGRESS_ICON.setVisible( true );
                                    checkBtn.setEnabled( false );
                                    startNStopButton.setEnabled( false );
                                    destroyBtn.setEnabled( false );

                                    executor.execute(
                                            new HBaseNodeOperationTask( hbase, tracker, config.getClusterName(), host,
                                                    NodeOperationType.STOP, role,
                                                    new io.subutai.plugin.common.api.CompleteEvent()
                                                    {
                                                        @Override
                                                        public void onComplete( final NodeState state )
                                                        {
                                                            synchronized ( PROGRESS_ICON )
                                                            {
                                                                PROGRESS_ICON.setVisible( false );
                                                                checkBtn.setEnabled( true );
                                                                checkBtn.click();
                                                            }
                                                        }
                                                    }, null ) );
                                }
                            }
                        } );
                        */
                        availableOperations.addComponent( destroyBtn );
                        destroyBtn.addClickListener( new Button.ClickListener()
                        {
                            @Override
                            public void buttonClick( final Button.ClickEvent clickEvent )
                            {
                                if ( config.getRegionServers().size() == 1 )
                                {
                                    show( "This is the last region server in the cluster. Please, destroy cluster "
                                            + "instead" );
                                    return;
                                }

                                ConfirmationDialog alert = new ConfirmationDialog(
                                        String.format( "Do you want to destroy %s node ?", host.getHostname() ), "Yes",
                                        "No" );
                                alert.getOk().addClickListener( new Button.ClickListener()
                                {
                                    @Override
                                    public void buttonClick( Button.ClickEvent clickEvent )
                                    {
                                        PROGRESS_ICON.setVisible( true );
                                        UUID track = hbase.destroyNode( config.getClusterName(), host.getHostname() );
                                        ProgressWindow window =
                                                new ProgressWindow( executor, tracker, track, HBaseConfig.PRODUCT_KEY );
                                        window.getWindow().addCloseListener( new Window.CloseListener()
                                        {
                                            @Override
                                            public void windowClose( Window.CloseEvent closeEvent )
                                            {
                                                PROGRESS_ICON.setVisible( false );
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

                    table.addItem( new Object[] {
                            host.getHostname(), host.getIpByInterfaceName( "eth0" ), role.name(), resultHolder,
                            availableOperations
                    }, null );

                    checkBtn.addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent event )
                        {
                            PROGRESS_ICON.setVisible( true );
                            checkBtn.setEnabled( false );
                            destroyBtn.setEnabled( false );
                            executor.execute( new HBaseNodeOperationTask( hbase, tracker, config.getClusterName(), host,
                                    NodeOperationType.STATUS, role,
                                    new io.subutai.plugin.common.api.CompleteEvent()
                                    {

                                        @Override
                                        public void onComplete( final NodeState state )
                                        {
                                            synchronized ( PROGRESS_ICON )
                                            {
                                                if ( state.equals( NodeState.RUNNING ) )
                                                {
                                                    checkBtn.setEnabled( true );
                                                    destroyBtn.setEnabled( true );
                                                }
                                                else if ( state.equals( NodeState.STOPPED ) )
                                                {
                                                    checkBtn.setEnabled( true );
                                                    destroyBtn.setEnabled( true );
                                                }
                                                else if ( state.equals( NodeState.UNKNOWN ) )
                                                {
                                                    checkBtn.setEnabled( true );
                                                    destroyBtn.setEnabled( true );
                                                }
                                                resultHolder.setValue( state.name() );
                                                PROGRESS_ICON.setVisible( false );
                                            }
                                        }
                                    }, null ) );
                        }
                    } );
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOGGER.error( "Container host not found", e );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment not found", e );
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


    private void refreshUI()
    {
        if ( config != null )
        {
            autoScaleBtn.setValue( config.isAutoScaling() );
            populateTable( nodesTable, config.getAllNodes() );
        }
        else
        {
            nodesTable.removeAllItems();
        }
    }


    public void refreshClustersInfo()
    {
        List<HBaseConfig> clusters = hbase.getClusters();
        HBaseConfig clusterInfo = ( HBaseConfig ) clusterCombo.getValue();
        clusterCombo.removeAllItems();
        if ( clusters != null && !clusters.isEmpty() )
        {
            for ( HBaseConfig info : clusters )
            {
                clusterCombo.addItem( info );
                clusterCombo.setItemCaption( info, info.getClusterName() + "(" + info.getHadoopClusterName() + ")" );
            }
            if ( clusterInfo != null )
            {
                for ( HBaseConfig c : clusters )
                {
                    if ( c.getClusterName().equals( clusterInfo.getClusterName() ) )
                    {
                        clusterCombo.setValue( c );
                        PROGRESS_ICON.setVisible( false );
                        return;
                    }
                }
            }
            else
            {
                clusterCombo.setValue( clusters.iterator().next() );
                PROGRESS_ICON.setVisible( false );
            }
        }
        PROGRESS_ICON.setVisible( false );
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

        table.setColumnExpandRatio( HOST_COLUMN_CAPTION, 0.1f );
        table.setColumnExpandRatio( IP_COLUMN_CAPTION, 0.1f );
        table.setColumnExpandRatio( NODE_ROLE_COLUMN_CAPTION, 0.15f );
        table.setColumnExpandRatio( STATUS_COLUMN_CAPTION, 0.25f );
        table.setColumnExpandRatio( AVAILABLE_OPERATIONS_COLUMN_CAPTION, 0.40f );
        table.addItemClickListener( getTableClickListener( table ) );
        return table;
    }


    protected ItemClickEvent.ItemClickListener getTableClickListener( final Table table )
    {
        return new ItemClickEvent.ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                if ( event.isDoubleClick() )
                {
                    String containerId =
                            ( String ) table.getItem( event.getItemId() ).getItemProperty( Manager.HOST_COLUMN_CAPTION )
                                            .getValue();
                    ContainerHost containerHost = null;
                    try
                    {
                        containerHost = environmentManager.findEnvironment(
                                hadoop.getCluster( config.getHadoopClusterName() ).getEnvironmentId() )
                                                          .getContainerHostByHostname( containerId );
                    }
                    catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Environment error", e );
                    }

                    if ( containerHost != null )
                    {
                        TerminalWindow terminal = new TerminalWindow( containerHost );
                        contentRoot.getUI().addWindow( terminal.getWindow() );
                    }
                    else
                    {
                        Notification.show( "Agent is not connected" );
                    }
                }
            }
        };
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
