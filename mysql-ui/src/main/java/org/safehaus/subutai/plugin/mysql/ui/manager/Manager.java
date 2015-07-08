package org.safehaus.subutai.plugin.mysql.ui.manager;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.CompleteEvent;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLC;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;
import org.safehaus.subutai.plugin.mysqlc.api.NodeOperationTask;
import org.safehaus.subutai.server.ui.component.ConfirmationDialog;
import org.safehaus.subutai.server.ui.component.ProgressWindow;
import org.safehaus.subutai.server.ui.component.TerminalWindow;

import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.event.ItemClickEvent;
import com.vaadin.server.Sizeable;
import com.vaadin.server.ThemeResource;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Table;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


/**
 * Created by tkila on 5/14/15.
 */
public class Manager extends VerticalLayout
{
    //@formatter:off
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION  = "AVAILABLE_OPTIONS";
    protected static final String START_ALL_BUTTON_CAPTION             = "Start Cluster";
    protected static final String STOP_ALL_BUTTON_CAPTION              = "Stop Cluster";
    protected static final String DESTROY_ALL_BUTTON_CAPTION           = "Destroy Cluster";
    protected static final String BUTTON_STYLE_NAME                    = "default";
    private   static final String DESTROY_NODE_BUTTON_CAPTION          = "Destroy";
    private   static final String CHECK_ALL_BUTTON_CAPTION             = "Check All" ;
    private   static final String ADD_DATA_NODE_BUTTON_CAPTION         = "Add data node" ;
    private   static final String ADD_MAST_NODE_BUTTON_CAPTION         = "Add master node" ;
    private   static final String ADD_SQL_NODE_BUTTON_CAPTION          = "Install MySQL API" ;
    protected static final String HOST_COLUMN_CAPTION                  = "Host";
    protected static final String IP_COLUMN_CAPTION                    = "IP List";
    protected static final String NODE_ROLE_COLUMN_CAPTION             = "Node Role";
    protected static final String STATUS_COLUMN_CAPTION                = "Status";
    private   static final String CHECK_BUTTON_CAPTION                 = "Check";
    private   static final Logger LOG      = Logger.getLogger( Manager.class.getName() );


    final         Button             startAll,
                                     stopAll,
                                     destroyAll,
                                     checkAll,
                                     addDataNodeBtn,
                                     addMasterNodeBtn;

    private final Embedded           PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );

    private final ExecutorService    executorService;
    private final Tracker            tracker;
    private final EnvironmentManager environmentManager;
    private final MySQLC             mySQLC;
    private final Table              nodesTable;
    private final Table              managersTable;
    private final GridLayout         contentRoot;
    private       MySQLClusterConfig config;
    private       ComboBox           clusterCombo;
   //@formatter:on


    public Manager( final ExecutorService executorService, final MySQLC mySQLC, final Tracker tracker,
                    final EnvironmentManager environmentManager )
    {
        this.mySQLC = mySQLC;
        this.executorService = executorService;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 25 );
        contentRoot.setColumns( 1 );

        nodesTable = createTableTemplate( "Data nodes" );
        nodesTable.setId( "SQLNodeTable" );

        managersTable = createTableTemplate( "Manager nodes" );
        managersTable.setId( "SQLManTable" );

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );
        controlsContent.setComponentAlignment( clusterNameLabel, Alignment.MIDDLE_CENTER );

        clusterCombo = new ComboBox();
        clusterCombo.setId( "SqlClusterComboBox" );
        clusterCombo.setImmediate( true );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                config = ( MySQLClusterConfig ) valueChangeEvent.getProperty().getValue();
                try
                {
                    refreshUI();
                    checkAllNodes();
                }
                catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                {
                    //show( e.getMessage() );
                }
            }
        } );

        controlsContent.addComponent( clusterCombo );
        controlsContent.setComponentAlignment( clusterCombo, Alignment.MIDDLE_CENTER );

        checkAll = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAll.setId( "SQLCheckAllBtn" );
        checkAll.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                synchronized ( PROGRESS_ICON )
                {
                    checkAllNodes();
                }
            }
        } );
        controlsContent.addComponent( checkAll );
        controlsContent.setComponentAlignment( checkAll, Alignment.MIDDLE_CENTER );
        //        start all button
        startAll = new Button( START_ALL_BUTTON_CAPTION );
        startAll.setId( "SQLStartAllBtn" );
        controlsContent.addComponent( startAll );
        controlsContent.setComponentAlignment( startAll, Alignment.MIDDLE_CENTER );

        startAll.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                synchronized ( PROGRESS_ICON )
                {
                    UUID uuid = mySQLC.startCluster( config.getClusterName() );
                    ProgressWindow window = new ProgressWindow( executorService, tracker, uuid, config.PRODUCT_KEY );
                    window.getWindow().addCloseListener( new Window.CloseListener()
                    {
                        @Override
                        public void windowClose( final Window.CloseEvent closeEvent )
                        {
                            checkAllNodes();
                        }
                    } );
                    contentRoot.getUI().addWindow( window.getWindow() );
                    disableButtons( startAll );
                    enableButtons( stopAll );
                }
            }
        } );

        stopAll = new Button( STOP_ALL_BUTTON_CAPTION );
        stopAll.setId( "SQLStopAllBtn" );

        controlsContent.addComponent( stopAll );
        controlsContent.setComponentAlignment( stopAll, Alignment.MIDDLE_CENTER );

        stopAll.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                PROGRESS_ICON.setVisible( true );
                synchronized ( PROGRESS_ICON )
                {
                    UUID uuid = mySQLC.stopCluster( config.getClusterName() );
                    ProgressWindow window = new ProgressWindow( executorService, tracker, uuid, config.PRODUCT_KEY );
                    window.getWindow().addCloseListener( new Window.CloseListener()
                    {
                        @Override
                        public void windowClose( final Window.CloseEvent closeEvent )
                        {

                            checkAllNodes();
                        }
                    } );
                    contentRoot.getUI().addWindow( window.getWindow() );
                    disableButtons( stopAll );
                    enableButtons( startAll );
                }
            }
        } );

        addDataNodeBtn = new Button( ADD_DATA_NODE_BUTTON_CAPTION );

        addDataNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to add Data Node to the %s. IT WILL RESTART CLUSTER!!!",
                                    config.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( final Button.ClickEvent clickEvent )
                        {
                            mySQLC.stopCluster( config.getClusterName() );
                            UUID trackID = mySQLC.addNode( config.getClusterName(), NodeType.DATANODE );
                            ProgressWindow window =
                                    new ProgressWindow( executorService, tracker, trackID, config.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( final Window.CloseEvent closeEvent )
                                {
                                    refreshClusterInfo();
                                    startAll.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );
                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
            }
        } );

        controlsContent.addComponent( addDataNodeBtn );
        controlsContent.setComponentAlignment( addDataNodeBtn, Alignment.MIDDLE_CENTER );
        addMasterNodeBtn = new Button( ADD_MAST_NODE_BUTTON_CAPTION );
        addMasterNodeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to add Master Node to the %s. IT WILL RESTART CLUSTER!!!",
                                    config.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( final Button.ClickEvent clickEvent )
                        {
                            mySQLC.stopCluster( config.getClusterName() );
                            UUID trackID = mySQLC.addNode( config.getClusterName(), NodeType.MASTER_NODE );
                            ProgressWindow window =
                                    new ProgressWindow( executorService, tracker, trackID, config.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( final Window.CloseEvent closeEvent )
                                {
                                    refreshClusterInfo();
                                    startAll.click();
                                    //checkAll.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );
                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
            }
        } );

        controlsContent.addComponent( addMasterNodeBtn );
        controlsContent.setComponentAlignment( addMasterNodeBtn, Alignment.MIDDLE_CENTER );

        destroyAll = new Button( DESTROY_ALL_BUTTON_CAPTION );
        destroyAll.setId( "SQLDestroyAllBtn" );

        destroyAll.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( config != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s cluster?", config.getClusterName() ), "Yes",
                            "No" );

                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( final Button.ClickEvent clickEvent )
                        {
                            mySQLC.stopCluster( config.getClusterName() );
                            UUID trackID = mySQLC.destroyCluster( config.getClusterName() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    MySQLClusterConfig.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( final Window.CloseEvent closeEvent )
                                {
                                    refreshClusterInfo();

                                    checkAll.click();
                                }
                            } );
                            contentRoot.getUI().addWindow( window.getWindow() );
                        }
                    } );
                    contentRoot.getUI().addWindow( alert.getAlert() );
                }
            }
        } );
        controlsContent.addComponent( destroyAll );
        controlsContent.setComponentAlignment( destroyAll, Alignment.MIDDLE_CENTER );

        addStyleNameToButtons( startAll, stopAll, destroyAll, checkAll, addDataNodeBtn, addMasterNodeBtn );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );

        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( nodesTable, 0, 1, 0, 5 );
        contentRoot.addComponent( managersTable, 0, 6, 0, 10 );
    }


    private void refreshUI() throws EnvironmentNotFoundException, ContainerHostNotFoundException
    {
        nodesTable.removeAllItems();
        managersTable.removeAllItems();

        if ( config != null )
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );


            Set<ContainerHost> dataNodes = new HashSet<>();
            Set<ContainerHost> managerNodes = new HashSet<>();

            for ( UUID uuid : config.getDataNodes() )
            {
                dataNodes.add( environment.getContainerHostById( uuid ) );
            }
            for ( UUID uuid : config.getManagerNodes() )
            {
                managerNodes.add( environment.getContainerHostById( uuid ) );
            }
            if ( environment != null )
            {

                populateTable( nodesTable, dataNodes, NodeType.DATANODE );
                populateTable( managersTable, managerNodes, NodeType.MASTER_NODE );
                populateTable( nodesTable, dataNodes, NodeType.SERVER );
            }
            else
            {
                show( String.format( "Could not get environment data" ) );
            }
        }
        else
        {
            nodesTable.removeAllItems();
            managersTable.removeAllItems();
        }
    }


    private void populateTable( final Table table, final Set<ContainerHost> containerHosts, final NodeType nodeType )
    {

        for ( final ContainerHost containerHost : containerHosts )
        {
            final Label resultHolder = new Label();
            resultHolder.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-sqlResult" );
            final Button destroyButton = new Button( DESTROY_NODE_BUTTON_CAPTION );
            destroyButton.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-sqlDestroy" );
            final Button checkButton = new Button( CHECK_BUTTON_CAPTION );
            checkButton.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-sqlCheck" );
            final Button installSqlButton = new Button( ADD_SQL_NODE_BUTTON_CAPTION );
            installSqlButton.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-sqlServer" );

            addStyleNameToButtons( checkButton, destroyButton, installSqlButton );

            PROGRESS_ICON.setVisible( false );
            final HorizontalLayout availableOptions = new HorizontalLayout();

            availableOptions.setStyleName( "default" );
            availableOptions.setSpacing( true );
            availableOptions.addComponent( checkButton );

            if ( !nodeType.name().equalsIgnoreCase( "server" ) )
            {
                availableOptions.addComponent( destroyButton );
            }
            else
            {
                if ( !config.getIsSqlInstalled().get( containerHost.getHostname() ) )
                {
                    disableButtons( checkButton );
                }
            }
            if ( nodeType.name().equalsIgnoreCase( "datanode" ) )
            {

                if ( !config.getIsSqlInstalled().get( containerHost.getHostname() ) )
                {
                    availableOptions.addComponent( installSqlButton );
                }
            }
            table.addItem( new Object[] {
                    containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ), nodeType.name(),
                    resultHolder, availableOptions
            }, null );
            installSqlButton.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( final Button.ClickEvent clickEvent )
                {
                    if ( config != null )
                    {
                        ConfirmationDialog alert = null;
                        if ( !config.getIsSqlInstalled().get( containerHost.getHostname() ) )
                        {
                            alert = new ConfirmationDialog(
                                    String.format( "Do you want to install SQL Server API on %s cluster?",
                                            config.getClusterName() ), "Yes", "No" );
                            alert.getOk().addClickListener( new Button.ClickListener()
                            {
                                @Override
                                public void buttonClick( final Button.ClickEvent clickEvent )
                                {

                                    UUID uuid = mySQLC.installSQLServer( config.getClusterName(), containerHost,
                                            NodeType.SERVER );
                                    ProgressWindow window = new ProgressWindow( executorService, tracker, uuid,
                                            MySQLClusterConfig.PRODUCT_KEY );
                                    disableButtons( installSqlButton );
                                    window.getWindow().addCloseListener( new Window.CloseListener()
                                    {
                                        @Override
                                        public void windowClose( final Window.CloseEvent closeEvent )
                                        {
                                            refreshClusterInfo();

                                            //checkAll.click();
                                        }
                                    } );
                                    contentRoot.getUI().addWindow( window.getWindow() );
                                }
                            } );

                            contentRoot.getUI().addWindow( alert.getAlert() );
                        }
                        else
                        {
                            disableButtons( installSqlButton );
                        }
                    }
                }
            } );
            checkButton.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( final Button.ClickEvent clickEvent )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( destroyButton, checkButton );
                    executorService.execute(
                            new NodeOperationTask( mySQLC, tracker, config.getClusterName(), containerHost,
                                    NodeOperationType.STATUS, new CompleteEvent()
                            {
                                @Override
                                public void onComplete( final NodeState state )
                                {
                                    synchronized ( PROGRESS_ICON )
                                    {
                                        resultHolder.setValue( state.name() );
                                        enableButtons( destroyButton, checkButton );
                                        PROGRESS_ICON.setVisible( false );
                                    }
                                }
                            }, null, nodeType ) );
                }
            } );
            destroyButton.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( final Button.ClickEvent clickEvent )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s node? IT WILL RESTART CLUSTER",
                                    containerHost.getHostname() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {

                        @Override
                        public void buttonClick( final Button.ClickEvent clickEvent )
                        {
                            mySQLC.stopCluster( config.getClusterName() );
                            UUID trackID = mySQLC.destroyService( config.getClusterName(), containerHost, nodeType );
                            ProgressWindow window =
                                    new ProgressWindow( executorService, tracker, trackID, config.PRODUCT_KEY );
                            window.getWindow().addCloseListener( new Window.CloseListener()
                            {
                                @Override
                                public void windowClose( final Window.CloseEvent closeEvent )
                                {
                                    refreshClusterInfo();
                                    checkAll.click();
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


    private void enableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( true );
        }
    }


    private void disableButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.setEnabled( false );
        }
    }


    private void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    public void checkAllNodes()
    {

        checkNodeStatus( nodesTable );
        checkNodeStatus( managersTable );
    }


    public void checkNodeStatus( Table table )
    {
        for ( Object object : table.getItemIds() )
        {
            int rowId = ( Integer ) object;
            Item row = table.getItem( rowId );
            HorizontalLayout availableOperations =
                    ( HorizontalLayout ) ( row.getItemProperty( AVAILABLE_OPERATIONS_COLUMN_CAPTION ).getValue() );
            if ( availableOperations != null )
            {

                Button checkBtn = getButton( availableOperations, CHECK_BUTTON_CAPTION );
                if ( checkBtn != null )
                {
                    checkBtn.click();
                }
            }
        }
    }


    private Button getButton( final HorizontalLayout availableOperations, final String caption )
    {
        if ( availableOperations == null )
        {
            return null;
        }
        else
        {
            for ( Component component : availableOperations )
            {
                if ( component.getCaption().equals( caption ) )
                {
                    return ( Button ) component;
                }
            }
            return null;
        }
    }


    private void show( final String message )
    {
        Notification.show( message );
    }


    private Table createTableTemplate( final String caption )
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
        table.setColumnCollapsingAllowed( true );
        table.addItemClickListener( getTableClickListener( table ) );
        return table;
    }


    private ItemClickEvent.ItemClickListener getTableClickListener( final Table table )
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
                        containerHost = environmentManager.findEnvironment( config.getEnvironmentId() )
                                                          .getContainerHostByHostname( containerId );
                    }
                    catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                    {
                        e.printStackTrace();
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


    public void refreshClusterInfo()
    {
        List<MySQLClusterConfig> info = mySQLC.getClusters();
        clusterCombo.removeAllItems();

        if ( !info.isEmpty() )
        {
            MySQLClusterConfig clusterInfo = ( MySQLClusterConfig ) clusterCombo.getValue();
            clusterCombo.removeAllItems();
            for ( MySQLClusterConfig mysqlInfo : info )
            {
                clusterCombo.addItem( mysqlInfo );
                clusterCombo.setItemCaption( mysqlInfo, mysqlInfo.getClusterName() );
            }
            if ( clusterInfo != null )
            {
                for ( MySQLClusterConfig mysqlInfo : info )
                {
                    if ( mysqlInfo.getClusterName().equals( clusterInfo.getClusterName() ) )
                    {
                        clusterCombo.setValue( mysqlInfo );
                        return;
                    }
                }
            }
            else
            {
                clusterCombo.setValue( info.iterator().next() );
            }
        }
    }


    public Component getContent()
    {
        return contentRoot;
    }
}

