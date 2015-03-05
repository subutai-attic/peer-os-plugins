/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.accumulo.ui.manager;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.accumulo.api.Accumulo;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
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
import com.vaadin.ui.Layout;
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
    protected static final String STOP_ALL_BUTTON_CAPTION = "Stop All";
    protected static final String DESTROY_BUTTON_CAPTION = "Destroy";
    protected static final String DESTROY_CLUSTER_BUTTON_CAPTION = "Destroy Cluster";
    protected static final String ADD_TRACER_BUTTON_CAPTION = "Add Tracer";
    protected static final String ADD_SLAVE_BUTTON_CAPTION = "Add Tablet Server";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String NODE_ROLE_COLUMN_CAPTION = "Node Role";
    protected static final String STATUS_COLUMN_CAPTION = "Status";
    protected static final String STYLE_NAME = "default";
    private static final Logger LOGGER = LoggerFactory.getLogger( Manager.class );
    private static final String AUTO_SCALE_BUTTON_CAPTION = "Auto Scale";
    private final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    private final GridLayout contentRoot;
    private final Table mastersTable;
    private final Table tracersTable;
    private final Table slavesTable;
    private final Accumulo accumulo;
    private final Hadoop hadoop;
    private final Zookeeper zookeeper;
    private final Tracker tracker;
    private final ExecutorService executorService;
    private final EnvironmentManager environmentManager;
    private ComboBox clusterCombo;
    private CheckBox autoScaleBtn;
    private AccumuloClusterConfig accumuloClusterConfig;
    private Button refreshClustersBtn, checkAllBtn, startClusterBtn, stopClusterBtn, destroyClusterBtn, addTracerBtn,
            addTabletServerButton;


    public Manager( final ExecutorService executorService, final Accumulo accumulo, Hadoop hadoop,
                    final Zookeeper zookeeper, Tracker tracker, EnvironmentManager environmentManager )
            throws NamingException
    {

        this.executorService = executorService;
        this.accumulo = accumulo;
        this.hadoop = hadoop;
        this.zookeeper = zookeeper;
        this.tracker = tracker;
        this.environmentManager = environmentManager;


        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 17 );
        contentRoot.setColumns( 1 );

        //tables go here
        mastersTable = createTableTemplate( "Masters" );
        mastersTable.setId( "MastersTable" );
        tracersTable = createTableTemplate( "Tracers" );
        tracersTable.setId( "TracersTable" );
        slavesTable = createTableTemplate( "Tablet Servers" );
        slavesTable.setId( "Slavestable" );

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );
        controlsContent.setHeight( 100, Sizeable.Unit.PERCENTAGE );

        Label clusterNameLabel = new Label( "Select the cluster" );
        controlsContent.addComponent( clusterNameLabel );
        controlsContent.setComponentAlignment( clusterNameLabel, Alignment.MIDDLE_CENTER );

        ComboBox clustersCombo = getClusterCombo();
        controlsContent.addComponent( clustersCombo );
        controlsContent.setComponentAlignment( clustersCombo, Alignment.MIDDLE_CENTER );

        refreshClustersBtn = getRefreshClustersButton();
        controlsContent.addComponent( refreshClustersBtn );
        controlsContent.setComponentAlignment( refreshClustersBtn, Alignment.MIDDLE_CENTER );

        checkAllBtn = getCheckAllButton();
        controlsContent.addComponent( checkAllBtn );
        controlsContent.setComponentAlignment( checkAllBtn, Alignment.MIDDLE_CENTER );

        startClusterBtn = getStartAllButton();
        controlsContent.addComponent( startClusterBtn );
        controlsContent.setComponentAlignment( startClusterBtn, Alignment.MIDDLE_CENTER );

        stopClusterBtn = getStopAllButton();
        controlsContent.addComponent( stopClusterBtn );
        controlsContent.setComponentAlignment( stopClusterBtn, Alignment.MIDDLE_CENTER );

        destroyClusterBtn = getDestroyClusterButton();
        controlsContent.addComponent( destroyClusterBtn );
        controlsContent.setComponentAlignment( destroyClusterBtn, Alignment.MIDDLE_CENTER );

        addTracerBtn = getAddTracerNodeButton();
        controlsContent.addComponent( addTracerBtn );
        controlsContent.setComponentAlignment( addTracerBtn, Alignment.MIDDLE_CENTER );

        addTabletServerButton = getAddSlaveButton();
        controlsContent.addComponent( addTabletServerButton );
        controlsContent.setComponentAlignment( addTabletServerButton, Alignment.MIDDLE_CENTER );

        //auto scale button
        autoScaleBtn = new CheckBox( AUTO_SCALE_BUTTON_CAPTION );
        autoScaleBtn.setValue( false );
        autoScaleBtn.addStyleName( "default" );
        controlsContent.addComponent( autoScaleBtn );
        controlsContent.setComponentAlignment( autoScaleBtn, Alignment.MIDDLE_CENTER );
        autoScaleBtn.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent event )
            {
                if ( accumuloClusterConfig == null )
                {
                    show( "Select cluster" );
                }
                else
                {
                    boolean value = ( Boolean ) event.getProperty().getValue();
                    accumuloClusterConfig.setAutoScaling( value );
                    try
                    {
                        accumulo.saveConfig( accumuloClusterConfig );
                    }
                    catch ( ClusterException e )
                    {
                        show( e.getMessage() );
                    }
                }
            }
        } );


        addStyleName( refreshClustersBtn, checkAllBtn, startClusterBtn, stopClusterBtn, destroyClusterBtn, addTracerBtn,
                addTabletServerButton );

        PROGRESS_ICON.setVisible( false );
        PROGRESS_ICON.setId( "indicator" );
        controlsContent.addComponent( PROGRESS_ICON );
        controlsContent.setComponentAlignment( PROGRESS_ICON, Alignment.MIDDLE_CENTER );

        contentRoot.addComponent( controlsContent, 0, 0 );
        contentRoot.addComponent( mastersTable, 0, 2, 0, 6 );
        contentRoot.addComponent( tracersTable, 0, 7, 0, 11 );
        contentRoot.addComponent( slavesTable, 0, 12, 0, 16 );
    }


    /**
     * Shows notification with the given argument
     *
     * @param notification notification which will shown.
     */
    private void show( String notification )
    {
        Notification.show( notification );
    }


    private ComboBox getClusterCombo()
    {
        clusterCombo = new ComboBox();
        clusterCombo.setId( "ClusterCb" );
        clusterCombo.setImmediate( true );
        clusterCombo.setTextInputAllowed( false );
        clusterCombo.setWidth( 200, Sizeable.Unit.PIXELS );
        clusterCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                accumuloClusterConfig = ( AccumuloClusterConfig ) event.getProperty().getValue();
                refreshUI();
            }
        } );
        return clusterCombo;
    }


    private Button getRefreshClustersButton()
    {
        refreshClustersBtn = new Button( REFRESH_CLUSTERS_CAPTION );
        refreshClustersBtn.setId( "ResfreshClustersBtn" );
        refreshClustersBtn.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                refreshClustersInfo();
            }
        } );
        return refreshClustersBtn;
    }


    private Button getCheckAllButton()
    {
        checkAllBtn = new Button( CHECK_ALL_BUTTON_CAPTION );
        checkAllBtn.setId( "CheckAllBtn" );
        checkAllBtn.addStyleName( "default" );
        checkAllBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                checkAll();
            }
        } );
        return checkAllBtn;
    }


    //    private HorizontalLayout getAddPropertyLayout()
    //    {
    //        HorizontalLayout customPropertyContent = new HorizontalLayout();
    //        customPropertyContent.setSpacing( true );
    //
    //        Label propertyNameLabel = new Label( "Property Name" );
    //        customPropertyContent.addComponent( propertyNameLabel );
    //        final TextField propertyNameTextField = new TextField();
    //        propertyNameTextField.setId( "propertyNameTxt" );
    //        customPropertyContent.addComponent( propertyNameTextField );
    //
    //        removePropertyBtn = new Button( "Remove" );
    //        removePropertyBtn.setId( "removePropertyBtn" );
    //        removePropertyBtn.addStyleName( "default" );
    //        removePropertyBtn.addClickListener( new Button.ClickListener()
    //        {
    //            @Override
    //            public void buttonClick( Button.ClickEvent clickEvent )
    //            {
    //                if ( accumuloClusterConfig != null )
    //                {
    //                    String propertyName = propertyNameTextField.getValue();
    //                    if ( Strings.isNullOrEmpty( propertyName ) )
    //                    {
    //                        Notification.show( "Please, specify property name to remove" );
    //                    }
    //                    else
    //                    {
    //                        UUID trackID = accumulo.removeProperty( accumuloClusterConfig.getClusterName(),
    // propertyName );
    //
    //                        ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
    //                                AccumuloClusterConfig.PRODUCT_KEY );
    //                        window.getWindow().addCloseListener( new Window.CloseListener()
    //                        {
    //                            @Override
    //                            public void windowClose( Window.CloseEvent closeEvent )
    //                            {
    //                                refreshClustersInfo();
    //                            }
    //                        } );
    //                        contentRoot.getUI().addWindow( window.getWindow() );
    //                    }
    //                }
    //                else
    //                {
    //                    Notification.show( "Please, select cluster" );
    //                }
    //            }
    //        } );
    //        customPropertyContent.addComponent( removePropertyBtn );
    //
    //        Label propertyValueLabel = new Label( "Property Value" );
    //        customPropertyContent.addComponent( propertyValueLabel );
    //        final TextField propertyValueTextField = new TextField();
    //        propertyValueTextField.setId( "propertyValueTxt" );
    //        customPropertyContent.addComponent( propertyValueTextField );
    //
    //        addPropertyBtn = new Button( "Add" );
    //        addPropertyBtn.setId( "addProperty" );
    //        addPropertyBtn.addStyleName( "default" );
    //        addPropertyBtn.addClickListener( new Button.ClickListener()
    //        {
    //            @Override
    //            public void buttonClick( Button.ClickEvent clickEvent )
    //            {
    //                if ( accumuloClusterConfig != null )
    //                {
    //                    String propertyName = propertyNameTextField.getValue();
    //                    String propertyValue = propertyValueTextField.getValue();
    //                    if ( Strings.isNullOrEmpty( propertyName ) )
    //                    {
    //                        Notification.show( "Please, specify property name to add" );
    //                    }
    //                    else if ( Strings.isNullOrEmpty( propertyValue ) )
    //                    {
    //                        Notification.show( "Please, specify property name to set" );
    //                    }
    //                    else
    //                    {
    //                        UUID trackID = accumulo.addProperty( accumuloClusterConfig.getClusterName(), propertyName,
    //                                propertyValue );
    //
    //                        ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
    //                                AccumuloClusterConfig.PRODUCT_KEY );
    //                        window.getWindow().addCloseListener( new Window.CloseListener()
    //                        {
    //                            @Override
    //                            public void windowClose( Window.CloseEvent closeEvent )
    //                            {
    //                                refreshClustersInfo();
    //                            }
    //                        } );
    //                        contentRoot.getUI().addWindow( window.getWindow() );
    //                    }
    //                }
    //                else
    //                {
    //                    Notification.show( "Please, select cluster" );
    //                }
    //            }
    //        } );
    //        customPropertyContent.addComponent( addPropertyBtn );
    //        return customPropertyContent;
    //    }


    private Button getAddSlaveButton()
    {
        addTabletServerButton = new Button( ADD_SLAVE_BUTTON_CAPTION );
        addTabletServerButton.setId( "addTabletServer" );
        addTabletServerButton.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( accumuloClusterConfig == null )
                {
                    Notification.show( "Select cluster" );
                    return;
                }
                Set<UUID> set = new HashSet<>(
                        hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getAllNodes() );
                set.removeAll( accumuloClusterConfig.getAllNodes() );
                if ( set.isEmpty() )
                {
                    Notification.show( "All nodes in Hadoop cluster have Accumulo installed" );
                    return;
                }


                ZookeeperClusterConfig zookeeperClusterConfig =
                        zookeeper.getCluster( accumuloClusterConfig.getZookeeperClusterName() );
                Set<UUID> nodesWithHadoopNZoo = new HashSet<>();
                for ( final UUID uuid : set )
                {
                    if ( zookeeperClusterConfig.getNodes().contains( uuid ) )
                    {
                        nodesWithHadoopNZoo.add( uuid );
                    }
                }

                Set<ContainerHost> myHostSet = new HashSet<>();
                for ( UUID uuid : nodesWithHadoopNZoo )
                {
                    try
                    {
                        myHostSet.add( environmentManager.findEnvironment(
                                hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getEnvironmentId() )
                                                         .getContainerHostById( uuid ) );
                    }
                    catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
                    {
                        LOGGER.error( "Error applying operation on environment/container" );
                    }
                }

                if ( myHostSet.isEmpty() )
                {
                    Notification
                            .show( "Please revise hadoop and zookeeper clusters for existence for additional nodes." );
                    return;
                }

                AddNodeWindow w =
                        new AddNodeWindow( accumulo, executorService, tracker, accumuloClusterConfig, myHostSet,
                                NodeType.ACCUMULO_TABLET_SERVER );
                contentRoot.getUI().addWindow( w );
                w.addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
                        refreshClustersInfo();
                        refreshUI();
                        checkAll();
                    }
                } );
            }
        } );
        return addTabletServerButton;
    }


    private Button getAddTracerNodeButton()
    {
        addTracerBtn = new Button( ADD_TRACER_BUTTON_CAPTION );
        addTracerBtn.setId( "addTracer" );
        addTracerBtn.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( accumuloClusterConfig == null )
                {
                    Notification.show( "Select cluster" );
                    return;
                }
                Set<UUID> set = new HashSet<>(
                        hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getAllNodes() );
                set.removeAll( accumuloClusterConfig.getAllNodes() );
                if ( set.isEmpty() )
                {
                    Notification.show( "All nodes in Hadoop cluster have Accumulo installed" );
                    return;
                }

                ZookeeperClusterConfig zookeeperClusterConfig =
                        zookeeper.getCluster( accumuloClusterConfig.getZookeeperClusterName() );
                Set<UUID> nodesWithHadoopNZoo = new HashSet<>();
                for ( final UUID uuid : set )
                {
                    if ( zookeeperClusterConfig.getNodes().contains( uuid ) )
                    {
                        nodesWithHadoopNZoo.add( uuid );
                    }
                }

                if ( nodesWithHadoopNZoo.isEmpty() )
                {
                    Notification.show( "None in Hadoop cluster have Zookeeper installed" );
                    return;
                }

                Set<ContainerHost> myHostSet = new HashSet<>();
                for ( UUID uuid : nodesWithHadoopNZoo )
                {
                    try
                    {
                        myHostSet.add( environmentManager.findEnvironment(
                                hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getEnvironmentId() )
                                                         .getContainerHostById( uuid ) );
                    }
                    catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Error applying operation on environment/container" );
                    }
                }

                if ( myHostSet.isEmpty() )
                {
                    Notification
                            .show( "Please revise hadoop and zookeeper clusters for existence for additional nodes." );
                    return;
                }

                AddNodeWindow w =
                        new AddNodeWindow( accumulo, executorService, tracker, accumuloClusterConfig, myHostSet,
                                NodeType.ACCUMULO_TRACER );
                contentRoot.getUI().addWindow( w );
                w.addCloseListener( new Window.CloseListener()
                {
                    @Override
                    public void windowClose( Window.CloseEvent closeEvent )
                    {
                        refreshClustersInfo();
                        refreshUI();
                        checkAll();
                    }
                } );
            }
        } );
        return addTracerBtn;
    }


    private Button getDestroyClusterButton()
    {
        destroyClusterBtn = new Button( DESTROY_CLUSTER_BUTTON_CAPTION );
        destroyClusterBtn.setId( "destroyClusterBtn" );
        destroyClusterBtn.addClickListener( new Button.ClickListener()
        {

            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                if ( accumuloClusterConfig != null )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s cluster?",
                                    accumuloClusterConfig.getClusterName() ), "Yes", "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            stopClusterBtn.click();
                            UUID trackID = accumulo.uninstallCluster( accumuloClusterConfig.getClusterName() );
                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    AccumuloClusterConfig.PRODUCT_KEY );
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
                    Notification.show( "Please, select cluster" );
                }
            }
        } );
        return destroyClusterBtn;
    }


    private Button getStartAllButton()
    {
        startClusterBtn = new Button( START_ALL_BUTTON_CAPTION );
        startClusterBtn.setId( "startAll" );
        startClusterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                PROGRESS_ICON.setVisible( true );
                executorService.execute(
                        new StartTask( accumulo, tracker, accumuloClusterConfig.getClusterName(), new CompleteEvent()
                        {
                            public void onComplete( String result )
                            {
                                synchronized ( PROGRESS_ICON )
                                {
                                    checkAll();
                                    PROGRESS_ICON.setVisible( false );
                                }
                            }
                        } ) );
            }
        } );
        return startClusterBtn;
    }


    private Button getStopAllButton()
    {
        stopClusterBtn = new Button( STOP_ALL_BUTTON_CAPTION );
        stopClusterBtn.setId( "stopAll" );
        stopClusterBtn.addStyleName( "default" );
        stopClusterBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                PROGRESS_ICON.setVisible( true );
                executorService.execute(
                        new StopTask( accumulo, tracker, accumuloClusterConfig.getClusterName(), new CompleteEvent()
                        {
                            public void onComplete( String result )
                            {
                                synchronized ( PROGRESS_ICON )
                                {
                                    checkAll();
                                    PROGRESS_ICON.setVisible( false );
                                }
                            }
                        } ) );
            }
        } );
        return stopClusterBtn;
    }


    public void checkAll()
    {
        checkNodesStatus( mastersTable );
        checkNodesStatus( tracersTable );
        checkNodesStatus( slavesTable );
    }


    private void checkNodesStatus( final Table nodesTable )
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


    private Button getButton( final HorizontalLayout availableOperationsLayout, String caption )
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

        addClickListenerToTable( table );

        return table;
    }


    private void addClickListenerToTable( final Table table )
    {
        table.addItemClickListener( new ItemClickEvent.ItemClickListener()
        {
            @Override
            public void itemClick( ItemClickEvent event )
            {
                String containerId =
                        ( String ) table.getItem( event.getItemId() ).getItemProperty( HOST_COLUMN_CAPTION ).getValue();
                ContainerHost containerHost = null;
                try
                {
                    containerHost = environmentManager.findEnvironment(
                            hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getEnvironmentId() )
                                                      .getContainerHostByHostname( containerId );
                }
                catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                {
                    LOGGER.error( "Error applying operations on environment/container" );
                }

                if ( containerHost != null )
                {
                    TerminalWindow terminal = new TerminalWindow( containerHost );
                    contentRoot.getUI().addWindow( terminal.getWindow() );
                }
                else
                {
                    Notification.show( "Host not found" );
                }
            }
        } );
    }


    private void refreshUI()
    {
        if ( accumuloClusterConfig != null )
        {
            Environment environment = null;
            try
            {
                environment = environmentManager.findEnvironment(
                        hadoop.getCluster( accumuloClusterConfig.getHadoopClusterName() ).getEnvironmentId() );


                populateTabletServersTable( slavesTable,
                        environment.getContainerHostsByIds( accumuloClusterConfig.getSlaves() ), false );

                populateTracersTable( tracersTable,
                        environment.getContainerHostsByIds( accumuloClusterConfig.getTracers() ), false );


                Set<ContainerHost> masters = new HashSet<>();
                masters.add( environment.getContainerHostById( accumuloClusterConfig.getMasterNode() ) );
                masters.add( environment.getContainerHostById( accumuloClusterConfig.getGcNode() ) );
                masters.add( environment.getContainerHostById( accumuloClusterConfig.getMonitor() ) );
                populateMastersTable( mastersTable, masters, true );
                autoScaleBtn.setValue( accumuloClusterConfig.isAutoScaling() );
            }
            catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
            {
                LOGGER.error( "Error applying operation on environment/container." );
            }
        }
        else
        {
            slavesTable.removeAllItems();
            tracersTable.removeAllItems();
            mastersTable.removeAllItems();
        }
    }


    private void populateMastersTable( final Table table, Set<ContainerHost> containerHosts, final boolean masters )
    {
        table.removeAllItems();
        for ( final ContainerHost containerHost : containerHosts )
        {
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            checkBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-accumuloCheck" );
            final Button destroyBtn = new Button( DESTROY_BUTTON_CAPTION );
            destroyBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-accumuloDestroy" );
            final Label resultHolder = new Label();
            resultHolder.setId( containerHost.getIpByInterfaceName( "eth0" ) + "accumuloResult" );

            HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.setSpacing( true );

            // TODO: think about adding destroy button !!!
            addGivenComponents( availableOperations, checkBtn );
            addStyleName( checkBtn, destroyBtn, availableOperations );

            List<NodeType> rolesOfNode = accumuloClusterConfig.getMasterNodeRoles( containerHost.getId() );
            for ( final NodeType role : rolesOfNode )
            {

                table.addItem( new Object[] {
                        containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ),
                        filterNodeRole( role.name() ), resultHolder, availableOperations
                }, null );

                checkBtn.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent event )
                    {
                        PROGRESS_ICON.setVisible( true );
                        disableButtons( checkBtn, destroyBtn );
                        executorService.execute(
                                new CheckTask( accumulo, tracker, accumuloClusterConfig.getClusterName(),
                                        containerHost.getHostname(), new CompleteEvent()
                                {
                                    public void onComplete( String result )
                                    {
                                        synchronized ( PROGRESS_ICON )
                                        {
                                            resultHolder.setValue( parseStatus( result, role ) );
                                            enableButtons( destroyBtn, checkBtn );
                                            PROGRESS_ICON.setVisible( false );
                                        }
                                    }
                                } ) );
                    }
                } );
            }

            destroyBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent event )
                {
                    ConfirmationDialog alert = new ConfirmationDialog(
                            String.format( "Do you want to destroy the %s node?", containerHost.getHostname() ), "Yes",
                            "No" );
                    alert.getOk().addClickListener( new Button.ClickListener()
                    {
                        @Override
                        public void buttonClick( Button.ClickEvent clickEvent )
                        {
                            UUID trackID = accumulo.destroyNode( accumuloClusterConfig.getClusterName(),
                                    containerHost.getHostname(), table == tracersTable ? NodeType.ACCUMULO_TRACER :
                                                                 NodeType.ACCUMULO_TABLET_SERVER );

                            ProgressWindow window = new ProgressWindow( executorService, tracker, trackID,
                                    AccumuloClusterConfig.PRODUCT_KEY );
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
    }


    private void populateTracersTable( final Table table, Set<ContainerHost> containerHosts, final boolean masters )
    {
        table.removeAllItems();
        for ( final ContainerHost containerHost : containerHosts )
        {
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            checkBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-accumuloCheck" );
            final Label resultHolder = new Label();
            resultHolder.setId( containerHost.getIpByInterfaceName( "eth0" ) + "accumuloResult" );

            HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.setSpacing( true );

            addGivenComponents( availableOperations, checkBtn );
            addStyleName( checkBtn, availableOperations );

            table.addItem( new Object[] {
                    containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ),
                    filterNodeRole( NodeType.ACCUMULO_TRACER.name() ), resultHolder, availableOperations
            }, null );

            checkBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent event )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( checkBtn );
                    executorService.execute( new CheckTask( accumulo, tracker, accumuloClusterConfig.getClusterName(),
                            containerHost.getHostname(), new CompleteEvent()
                    {
                        public void onComplete( String result )
                        {
                            synchronized ( PROGRESS_ICON )
                            {
                                resultHolder.setValue( parseStatus( result, NodeType.ACCUMULO_TRACER ) );
                                enableButtons( checkBtn );
                                PROGRESS_ICON.setVisible( false );
                            }
                        }
                    } ) );
                }
            } );
        }
    }


    private void populateTabletServersTable( final Table table, Set<ContainerHost> containerHosts,
                                             final boolean masters )
    {
        table.removeAllItems();
        for ( final ContainerHost containerHost : containerHosts )
        {
            final Button checkBtn = new Button( CHECK_BUTTON_CAPTION );
            checkBtn.setId( containerHost.getIpByInterfaceName( "eth0" ) + "-accumuloCheck" );
            final Label resultHolder = new Label();
            resultHolder.setId( containerHost.getIpByInterfaceName( "eth0" ) + "accumuloResult" );

            HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.setSpacing( true );

            addGivenComponents( availableOperations, checkBtn );
            addStyleName( checkBtn, availableOperations );

            table.addItem( new Object[] {
                    containerHost.getHostname(), containerHost.getIpByInterfaceName( "eth0" ),
                    filterNodeRole( NodeType.ACCUMULO_TABLET_SERVER.name() ), resultHolder, availableOperations
            }, null );

            checkBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( Button.ClickEvent event )
                {
                    PROGRESS_ICON.setVisible( true );
                    disableButtons( checkBtn );
                    executorService.execute( new CheckTask( accumulo, tracker, accumuloClusterConfig.getClusterName(),
                            containerHost.getHostname(), new CompleteEvent()
                    {
                        public void onComplete( String result )
                        {
                            synchronized ( PROGRESS_ICON )
                            {
                                resultHolder.setValue( parseStatus( result, NodeType.ACCUMULO_TABLET_SERVER ) );
                                enableButtons( checkBtn );
                                PROGRESS_ICON.setVisible( false );
                            }
                        }
                    } ) );
                }
            } );
        }
    }


    private String filterNodeRole( String role )
    {
        int index = role.indexOf( "_" );
        return role.substring( ( index + 1 ), role.length() );
    }


    private void addStyleName( Component... components )
    {
        for ( Component c : components )
        {
            c.addStyleName( STYLE_NAME );
        }
    }


    private void addGivenComponents( Layout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
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


    private String parseStatus( String result, NodeType role )
    {
        StringBuilder sb = new StringBuilder();
        String roleString = convertEnumValues( role );
        if ( result.contains( "not connected" ) )
        {
            sb.append( "Container is not connected !" );
            return sb.toString();
        }
        String results[] = result.split( "\n" );
        for ( String part : results )
        {

            if ( part.toLowerCase().contains( roleString.toLowerCase() ) )
            {
                return part;
            }
        }
        return null;
    }


    private String convertEnumValues( NodeType role )
    {
        switch ( role )
        {
            case ACCUMULO_MASTER:
                return "Master";
            case ACCUMULO_GC:
                return "GC";
            case ACCUMULO_MONITOR:
                return "Monitor";
            case ACCUMULO_TABLET_SERVER:
                return "Tablet Server";
            case ACCUMULO_TRACER:
                return "Accumulo Tracer";
            case ACCUMULO_LOGGER:
                return "Logger";
        }
        return null;
    }


    public void refreshClustersInfo()
    {
        List<AccumuloClusterConfig> mongoClusterInfos = accumulo.getClusters();
        AccumuloClusterConfig clusterInfo = ( AccumuloClusterConfig ) clusterCombo.getValue();
        clusterCombo.removeAllItems();
        if ( mongoClusterInfos != null && !mongoClusterInfos.isEmpty() )
        {
            for ( AccumuloClusterConfig accumuloCluster : mongoClusterInfos )
            {
                clusterCombo.addItem( accumuloCluster );
                clusterCombo.setItemCaption( accumuloCluster,
                        accumuloCluster.getClusterName() + "(" + accumuloCluster.getHadoopClusterName() + ")" );
            }
            if ( clusterInfo != null )
            {
                for ( AccumuloClusterConfig mongoClusterInfo : mongoClusterInfos )
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
                clusterCombo.setValue( mongoClusterInfos.iterator().next() );
            }
        }
        refreshUI();
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
