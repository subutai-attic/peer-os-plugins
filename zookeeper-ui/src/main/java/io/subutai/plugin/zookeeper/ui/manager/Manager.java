package io.subutai.plugin.zookeeper.ui.manager;


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
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.NodeOperationTask;
import io.subutai.plugin.zookeeper.api.SetupType;
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

    private static final Logger LOGGER = LoggerFactory.getLogger( Manager.class );
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION = "AVAILABLE_OPERATIONS";
    protected static final String REFRESH_CLUSTERS_CAPTION = "Refresh Clusters";
    protected static final String CHECK_ALL_BUTTON_CAPTION = "Check All";
    protected static final String CHECK_BUTTON_CAPTION = "Check";
    protected static final String START_ALL_BUTTON_CAPTION = "Start All";
    protected static final String START_BUTTON_CAPTION = "Start";
    protected static final String STOP_ALL_BUTTON_CAPTION = "Stop All";
    protected static final String STOP_BUTTON_CAPTION = "Stop";
    protected static final String START_STOP_BUTTON_CAPTION = "Start/Stop";
    protected static final String DESTROY_BUTTON_CAPTION = "Destroy";
    protected static final String DESTROY_CLUSTER_BUTTON_CAPTION = "Destroy Cluster";
    protected static final String ADD_NODE_BUTTON_CAPTION = "Add Node";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String STATUS_COLUMN_CAPTION = "Status";
    protected static final String BUTTON_STYLE_NAME = "default";
    private static final String MESSAGE = "No cluster is installed !";
    private static final String AUTO_SCALE_BUTTON_CAPTION = "Auto Scale";
    final Button refreshClustersBtn, startAllBtn, stopAllBtn, checkAllBtn, destroyClusterBtn, addNodeBtn;
    private final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    private final GridLayout contentRoot;
    private final ComboBox clusterCombo;
    private final Table nodesTable;
    private final Hadoop hadoop;
    private final Zookeeper zookeeper;
    private final EnvironmentManager environmentManager;
    private final Tracker tracker;
    private final ExecutorService executorService;
    private ZookeeperClusterConfig config;
    private CheckBox autoScaleBtn;


    public Manager( final ExecutorService executorService, final Zookeeper zookeeper, final Hadoop hadoop,
                    final Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {

        this.executorService = executorService;
        this.zookeeper = zookeeper;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 11 );
        contentRoot.setColumns( 1 );

        //tables go here
        nodesTable = createTableTemplate( "Nodes" );
        nodesTable.setId( "ZookeeperMngNodesTable" );
        contentRoot.setId( "ZookeeperMngContentRoot" );
        //tables go here

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );
        controlsContent.setComponentAlignment( clusterNameLabel, Alignment.MIDDLE_CENTER );

        clusterCombo = new ComboBox();
        clusterCombo.setId( "ZookeeperMngClusterCombo" );
        clusterCombo.setImmediate( true );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                config = ( ZookeeperClusterConfig ) event.getProperty().getValue();
                refreshUI();
                checkNodesStatus();
            }
        } );
        controlsContent.addComponent( clusterCombo );
        controlsContent.setComponentAlignment( clusterCombo, Alignment.MIDDLE_CENTER );

        refreshClustersBtn = new Button( REFRESH_CLUSTERS_CAPTION );
        refreshClustersBtn.setId( "ZookeeperMngRefresh" );
        addClickListener( refreshClustersBtn );
        controlsContent.addComponent( refreshClustersBtn );
        controlsContent.setComponentAlignment( refreshClustersBtn, Alignment.MIDDLE_CENTER );

        checkAllBtn = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAllBtn.setId( "ZookeeperMngCheckAll" );
        addClickListener( checkAllBtn );
        controlsContent.addComponent( checkAllBtn );
        controlsContent.setComponentAlignment( checkAllBtn, Alignment.MIDDLE_CENTER );

        startAllBtn = new Button( START_ALL_BUTTON_CAPTION );
        startAllBtn.setId( "ZookeeperMngStartAll" );
        addClickListener( startAllBtn );
        controlsContent.addComponent( startAllBtn );
        controlsContent.setComponentAlignment( startAllBtn, Alignment.MIDDLE_CENTER );

        stopAllBtn = new Button( STOP_ALL_BUTTON_CAPTION );
        stopAllBtn.setId( "ZookeeperMngStopAll" );
        addClickListener( stopAllBtn );
        controlsContent.addComponent( stopAllBtn );
        controlsContent.setComponentAlignment( stopAllBtn, Alignment.MIDDLE_CENTER );

        destroyClusterBtn = new Button( DESTROY_CLUSTER_BUTTON_CAPTION );
        destroyClusterBtn.setId( "ZookeeperMngDestroyCluster" );
        addClickListenerToDestroyButton();
        controlsContent.addComponent( destroyClusterBtn );
        controlsContent.setComponentAlignment( destroyClusterBtn, Alignment.MIDDLE_CENTER );

        addNodeBtn = new Button( ADD_NODE_BUTTON_CAPTION );
        addNodeBtn.setId( "ZookeeperMngAddNode" );
        addClickListenerToAddNodeButton();
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
                        zookeeper.saveConfig( config );
                    }
                    catch ( ClusterException e )
                    {
                        show( e.getMessage() );
                    }
                }
            }
        } );

        addStyleNameToButtons( refreshClustersBtn, checkAllBtn, startAllBtn, stopAllBtn, destroyClusterBtn,
                addNodeBtn );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );
        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( nodesTable, 0, 2, 0, 10 );
    }


    public void addClickListener( Button button )
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
                            checkNodesStatus();
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


    public void startAllNodes()
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
                    Button startBtn = getButton( availableOperationsLayout, START_BUTTON_CAPTION );
                    if ( startBtn != null )
                    {
                        startBtn.click();
                    }
                }
            }
        }
    }


    public void stopAllNodes()
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
                    Button stopBtn = getButton( availableOperationsLayout, STOP_BUTTON_CAPTION );
                    if ( stopBtn != null )
                    {
                        stopBtn.click();
                    }
                }
            }
        }
    }


    public void addClickListenerToDestroyButton()
    {
        destroyClusterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s cluster?", config.getClusterName() ), "Yes",
                            "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID trackID = zookeeper.uninstallCluster( config.getClusterName() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    ZookeeperClusterConfig.PRODUCT_KEY );
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


    public void addClickListenerToAddNodeButton()
    {
        addNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( config != null )
                {
                    if ( config.getSetupType() == SetupType.STANDALONE
                            || config.getSetupType() == SetupType.OVER_ENVIRONMENT )
                    {
                        Environment environment;
                        try
                        {
                            environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                        }
                        catch ( EnvironmentNotFoundException e )
                        {
                            LOGGER.error( String.format( "Couldn't get environment with id: %s",
                                    config.getEnvironmentId().toString() ), e );
                            return;
                        }
                        Set<ContainerHost> environmentHosts = new HashSet<>( environment.getContainerHosts() );
                        Set<ContainerHost> zookeeperHosts = new HashSet<>();
                        try
                        {
                            zookeeperHosts.addAll( environment.getContainerHostsByIds( config.getNodes() ) );
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            LOGGER.error( String.format( "Couldn't get some container hosts with ids: %s",
                                    config.getNodes().toString() ), e );
                            return;
                        }
                        environmentHosts.removeAll( zookeeperHosts );
                        AddNodeWindow addNodeWindow =
                                new AddNodeWindow( zookeeper, executorService, tracker, config, environmentHosts );
                        contentRoot.getUI().addWindow( addNodeWindow );
                        addNodeWindow.addCloseListener( new Window.CloseListener()
                        {
                            @Override
                            public void windowClose( Window.CloseEvent closeEvent )
                            {
                                refreshClustersInfo();
                            }
                        } );
                    }
                    else if ( config.getSetupType() == SetupType.OVER_HADOOP
                            || config.getSetupType() == SetupType.WITH_HADOOP )
                    {
                        HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );

                        if ( hadoopClusterConfig != null )
                        {
                            Environment hadoopEnvironment;
                            try
                            {
                                hadoopEnvironment =
                                        environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() );
                            }
                            catch ( EnvironmentNotFoundException e )
                            {
                                LOGGER.error( String.format( "Couldn't get environment with id: %s",
                                        hadoopClusterConfig.getEnvironmentId().toString() ), e );
                                return;
                            }
                            Set<UUID> hadoopNodeIDs = new HashSet<>( hadoopClusterConfig.getAllNodes() );
                            Set<ContainerHost> hadoopNodes = new HashSet<>();
                            Set<ContainerHost> zookeeperHosts = new HashSet<>();

                            for ( final UUID nodeId : config.getNodes() )
                            {
                                try
                                {
                                    zookeeperHosts.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                                }
                                catch ( ContainerHostNotFoundException e )
                                {
                                    LOGGER.warn( String.format( "Couldn't get some container host with ids: %s",
                                            nodeId.toString() ), e );
                                }
                            }

                            for ( final UUID nodeId : hadoopNodeIDs )
                            {
                                try
                                {
                                    hadoopNodes.add( hadoopEnvironment.getContainerHostById( nodeId ) );
                                }
                                catch ( ContainerHostNotFoundException e )
                                {
                                    LOGGER.warn( String.format( "Couldn't get some container host with ids: %s",
                                            nodeId.toString() ), e );
                                }
                            }

                            Set<ContainerHost> nodes = new HashSet<>();
                            nodes.addAll( hadoopNodes );
                            nodes.removeAll( zookeeperHosts );
                            if ( !nodes.isEmpty() )
                            {
                                AddNodeWindow addNodeWindow =
                                        new AddNodeWindow( zookeeper, executorService, tracker, config, nodes );
                                contentRoot.getUI().addWindow( addNodeWindow );
                                addNodeWindow.addCloseListener( new Window.CloseListener()
                                {
                                    @Override
                                    public void windowClose( Window.CloseEvent closeEvent )
                                    {
                                        refreshClustersInfo();
                                    }
                                } );
                            }
                            else
                            {
                                show( "All nodes in corresponding Hadoop cluster have Zookeeper installed" );
                            }
                        }
                        else
                        {
                            show( "Hadoop cluster hadoopClusterConfig not found" );
                        }
                    }
                }
                else
                {
                    show( "Please, select cluster" );
                }
            }
        } );
    }


    private Table createTableTemplate( String caption )
    {
        final Table table = new Table( caption );
        table.addContainerProperty( HOST_COLUMN_CAPTION, String.class, null );
        table.addContainerProperty( IP_COLUMN_CAPTION, String.class, null );
        table.addContainerProperty( STATUS_COLUMN_CAPTION, Label.class, null );
        table.addContainerProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION, HorizontalLayout.class, null );
        table.setSizeFull();
        table.setPageLength( 10 );
        table.setSelectable( false );
        table.setImmediate( true );

        addItemClickListenerToTable( table );
        return table;
    }


    public void addItemClickListenerToTable( final Table table )
    {
        table.addItemClickListener( new ItemClickEvent.ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                if ( event.isDoubleClick() )
                {
                    String lxcHostname =
                            ( String ) table.getItem( event.getItemId() ).getItemProperty( HOST_COLUMN_CAPTION )
                                            .getValue();
                    try
                    {

                        Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                        ContainerHost containerHost = environment.getContainerHostByHostname( lxcHostname );
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
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( String.format( "Couldn't find environment with id: %s",
                                config.getEnvironmentId().toString() ), e );
                    }
                    catch ( ContainerHostNotFoundException e )
                    {
                        LOGGER.error( String.format( "Container host with name: %s is empty", lxcHostname ), e );
                    }
                }
            }
        } );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }


    private void refreshUI()
    {
        if ( config != null )
        {
            Environment environment;
            try
            {
                environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( String.format( "Environment with id: %s is null", config.getEnvironmentId().toString() ),
                        e );
                return;
            }

            populateTable( nodesTable, getZookeeperNodes( environment.getContainerHosts() ) );
            autoScaleBtn.setValue( config.isAutoScaling() );
        }
        else
        {
            nodesTable.removeAllItems();
        }
    }


    private Set<ContainerHost> getZookeeperNodes( final Set<ContainerHost> containerHosts )
    {
        Set<ContainerHost> list = new HashSet<>();
        for ( ContainerHost containerHost : containerHosts )
        {
            if ( config.getNodes().contains( containerHost.getId() ) )
            {
                list.add( containerHost );
            }
        }
        return list;
    }


    private void populateTable( final Table table, Set<ContainerHost> containerHosts )
    {
        table.removeAllItems();
        for ( final ContainerHost containerHost : containerHosts )
        {
            final Label resultHolder = new Label();
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            final Button startNstopButton = new Button( START_STOP_BUTTON_CAPTION );

            final Button destroyBtn = new Button( DESTROY_BUTTON_CAPTION );

            checkBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-zookeeperCheck" );
            destroyBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-zookeeperDestroy" );
            startNstopButton.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-zookeeperStartStop" );

            HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.setSpacing( true );
            availableOperations.addStyleName( "default" );

            addGivenComponents( availableOperations, checkBtn, startNstopButton, destroyBtn );
            addStyleNameToButtons( checkBtn, destroyBtn, startNstopButton );

            PROGRESS_ICON.setVisible( false );

            table.addItem( new Object[] {
                    containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ), resultHolder,
                    availableOperations
            }, null );

            checkBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent event )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( startNstopButton, checkBtn, destroyBtn );
                    executorService.execute(
                            new NodeOperationTask( zookeeper, tracker, config.getClusterName(), containerHost,
                                    NodeOperationType.STATUS, new CompleteEvent()
                            {
                                @Override
                                public void onComplete( NodeState nodeState )
                                {
                                    synchronized ( PROGRESS_ICON )
                                    {
                                        if ( nodeState.equals( NodeState.RUNNING ) )
                                        {
                                            startNstopButton.setEnabled( true );
                                            startNstopButton.setCaption( STOP_BUTTON_CAPTION );
                                        }
                                        else if ( nodeState.equals( NodeState.STOPPED ) )
                                        {
                                            startNstopButton.setEnabled( true );
                                            startNstopButton.setCaption( START_BUTTON_CAPTION );
                                        }
                                        else if ( nodeState.equals( NodeState.UNKNOWN ) )
                                        {
                                            startNstopButton.setEnabled( true );
                                            startNstopButton.setCaption( START_STOP_BUTTON_CAPTION );
                                        }
                                        resultHolder.setValue( nodeState.name() );
                                        PROGRESS_ICON.setVisible( false );
                                        checkBtn.setEnabled( true );
                                        destroyBtn.setEnabled( true );
                                    }
                                }
                            }, null ) );
                }
            } );

            addStartNStopButtonClickListener( containerHost, startNstopButton, destroyBtn, checkBtn );
            addDestroyButtonClickListener( containerHost, destroyBtn, checkBtn, destroyBtn );
        }
    }


    public void disableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( false );
        }
    }


    public void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    public void addDestroyButtonClickListener( final ContainerHost containerHost, final Button... buttons )
    {
        getButton( DESTROY_BUTTON_CAPTION, buttons ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( config.getNodes().size() == 1 )
                {
                    show( "This is the last node in cluster, please destroy whole cluster !" );
                    return;
                }

                ConfirmationDialog alert = new ConfirmationDialog(
                        String.format( "Do you want to destroy the %s node?", containerHost.getHostname() ), "Yes",
                        "No" );
                alert.getOk().addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent clickEvent )
                    {
                        UUID trackID = zookeeper.destroyNode( config.getClusterName(), containerHost.getHostname() );
                        ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                ZookeeperClusterConfig.PRODUCT_KEY );
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
        } );
    }


    public void refreshClustersInfo()
    {
        List<ZookeeperClusterConfig> zookeeperClusterConfigs = zookeeper.getClusters();
        ZookeeperClusterConfig clusterInfo = ( ZookeeperClusterConfig ) clusterCombo.getValue();
        clusterCombo.removeAllItems();
        if ( zookeeperClusterConfigs != null && !zookeeperClusterConfigs.isEmpty() )
        {
            for ( ZookeeperClusterConfig zookeeperClusterConfig : zookeeperClusterConfigs )
            {
                clusterCombo.addItem( zookeeperClusterConfig );
                clusterCombo.setItemCaption( zookeeperClusterConfig, zookeeperClusterConfig.getClusterName() );
            }
            if ( clusterInfo != null )
            {
                for ( ZookeeperClusterConfig mongoClusterInfo : zookeeperClusterConfigs )
                {
                    if ( mongoClusterInfo.getClusterName().equals( clusterInfo.getClusterName() ) )
                    {
                        clusterCombo.setValue( mongoClusterInfo );
                        return;
                    }
                }
            }
            else
            {
                clusterCombo.setValue( zookeeperClusterConfigs.iterator().next() );
            }
        }
    }


    public void addStartNStopButtonClickListener( final ContainerHost containerHost, final Button... buttons )
    {
        final Button startNstopButton = getButton( START_STOP_BUTTON_CAPTION, buttons );
        startNstopButton.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                disableButtons( buttons );
                if ( startNstopButton.getCaption().equals( START_BUTTON_CAPTION ) ){
                    executorService.execute(
                            new NodeOperationTask( zookeeper, tracker, config.getClusterName(), containerHost,
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
                else if ( startNstopButton.getCaption().equals( STOP_BUTTON_CAPTION ) ){
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( buttons );
                    executorService.execute(
                            new NodeOperationTask( zookeeper, tracker, config.getClusterName(), containerHost,
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
            }
        } );
    }


    public Button getButton( String caption, Button... buttons )
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


    public void addGivenComponents( HorizontalLayout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
        }
    }


    public void checkNodesStatus()
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


    public Component getContent()
    {
        return contentRoot;
    }
}
