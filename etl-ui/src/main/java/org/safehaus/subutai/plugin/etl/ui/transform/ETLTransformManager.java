package org.safehaus.subutai.plugin.etl.ui.transform;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.etl.ui.UIUtil;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class ETLTransformManager
{
    private final GridLayout contentRoot;
    private final ETL sqoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;

    private ETLConfig config;
    private Environment environment;
    private Hadoop hadoop;
//    public final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    public ProgressBar progressBar;

    private int NODE_SELECTION_GRID_LAYOUT_COL_INDEX = 0;
    private int NODE_SELECTION_GRID_LAYOUT_ROW_INDEX = 2;

    QueryType type;
    HorizontalLayout hadoopComboWithProgressIcon;


    public ETLTransformManager( ExecutorService executorService, ETL sqoop, Hadoop hadoop, Tracker tracker,
                                EnvironmentManager environmentManager )
            throws NamingException
    {
        this.executorService = executorService;
        this.sqoop = sqoop;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 20 );
        contentRoot.setColumns( 20 );

        init( contentRoot, type );
    }


    public void init( final GridLayout gridLayout, QueryType type ){
        gridLayout.removeAllComponents();

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );

        Label clusterNameLabel = new Label( "Select Sqoop installation:" );
        controlsContent.addComponent( clusterNameLabel );

        hadoopComboWithProgressIcon = new HorizontalLayout();
        hadoopComboWithProgressIcon.setSpacing( true );

        ComboBox hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setNullSelectionAllowed( false );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );

        hadoopComboWithProgressIcon.addComponent( hadoopClustersCombo );

        progressBar = new ProgressBar();
        progressBar.setId( "indicator" );
        progressBar.setIndeterminate( true );
        progressBar.setVisible( true );

        hadoopComboWithProgressIcon.addComponent( progressBar );
        hadoopComboWithProgressIcon.setComponentAlignment( progressBar, Alignment.BOTTOM_CENTER );
        contentRoot.addComponent( hadoopComboWithProgressIcon, 0, 1 );

        List<HadoopClusterConfig> clusters = hadoop.getClusters();
        if ( !clusters.isEmpty() )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClustersCombo.addItem( hadoopClusterInfo );
                hadoopClustersCombo.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        final VerticalLayout layout = new VerticalLayout();
        layout.addComponent( UIUtil.getLabel( "Select query type<br/>", 200 ) );

        TabSheet tabsheet = new TabSheet();

        VerticalLayout tab1 = new VerticalLayout();
        tab1.setCaption( QueryType.HIVE.name() );
        QueryPanel tab1Panel = new QueryPanel();
        tab1.addComponent( tab1Panel );
        tabsheet.addTab(tab1);

        VerticalLayout tab2 = new VerticalLayout();
        tab2.setCaption( QueryType.PIG.name() );
        QueryPanel tab2Panel = new QueryPanel();
        tab2.addComponent( tab2Panel );
        tabsheet.addTab(tab2);

        if ( type == null ){
            type = QueryType.HIVE;
        }

        final ComboBox hiveSelection = new ComboBox( "Select Hive Installation" );
        hiveSelection.setNullSelectionAllowed( false );
        hiveSelection.setImmediate( true );
        hiveSelection.setTextInputAllowed( false );
        hiveSelection.setRequired( true );

        final ComboBox pigSelection = new ComboBox( "Select Pig Installation" );
        pigSelection.setNullSelectionAllowed( false );
        pigSelection.setImmediate( true );
        pigSelection.setTextInputAllowed( false );
        pigSelection.setRequired( true );

        switch ( type ){
            case HIVE:
                tabsheet.setSelectedTab( tab1 );
                contentRoot.addComponent( hiveSelection, NODE_SELECTION_GRID_LAYOUT_COL_INDEX, NODE_SELECTION_GRID_LAYOUT_ROW_INDEX );
                break;
            case PIG:
                tabsheet.setSelectedTab( tab2 );
                contentRoot.addComponent( pigSelection, NODE_SELECTION_GRID_LAYOUT_COL_INDEX, NODE_SELECTION_GRID_LAYOUT_ROW_INDEX );
                break;
        }

        tabsheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {
            @Override
            public void selectedTabChange( TabSheet.SelectedTabChangeEvent event )
            {
                TabSheet tabsheet = event.getTabSheet();
                String caption = tabsheet.getTab( event.getTabSheet().getSelectedTab() ).getCaption();
                if ( caption.equals( QueryType.HIVE.name() ) )
                {
                    init( gridLayout, QueryType.HIVE );
                }
                else if ( caption.equals( QueryType.PIG.name() ) )
                {
                    init( gridLayout, QueryType.PIG );
                }
            }
        } );
        layout.addComponent(tabsheet);


        // event listeners
        final QueryType queryType = type;
        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
                    Set<ContainerHost> hadoopNodes =
                            hadoopEnvironment.getContainerHostsByIds( Sets.newHashSet( hadoopInfo.getAllNodes() ) );

                    hiveSelection.setValue( null );
                    pigSelection.setValue( null );
                    hiveSelection.removeAllItems();
                    pigSelection.removeAllItems();

                    hadoopComboWithProgressIcon.getComponent( 1 ).setVisible( true );
                    Set<ContainerHost> filteredHadoopNodes = filterHadoopNodes( hadoopNodes, queryType );
                    if ( filteredHadoopNodes.isEmpty() ){
                        show( "No node has subutai " + queryType.name() + " package installed" );
                    }
                    else {
                        for ( ContainerHost hadoopNode : filterHadoopNodes( hadoopNodes, queryType ) )
                        {
                            hiveSelection.addItem( hadoopNode );
                            hiveSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );

                            pigSelection.addItem( hadoopNode );
                            pigSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                        }
                    }
                    hadoopComboWithProgressIcon.getComponent( 1 ).setVisible( false );
                }
            }
        } );

        hiveSelection.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    ContainerHost containerHost = ( ContainerHost ) event.getProperty().getValue();
                }
            }
        } );

        pigSelection.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    ContainerHost containerHost = ( ContainerHost ) event.getProperty().getValue();
                }
            }
        } );

        gridLayout.addComponent( layout, 1, 0, 18, 17 );
    }

    public void show( String message ){
        Notification.show( message );
    }



    public void showProgress(){
        progressBar.setVisible( true );
    }


    public void hideProgress()
    {
        progressBar.setVisible( false );
    }

    private Set<ContainerHost> filterHadoopNodes( Set<ContainerHost> containerHosts, QueryType type ){
        Set<ContainerHost> resultList = new HashSet<>();
        for( ContainerHost host : containerHosts ){
            String command = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            try
            {
                CommandResult result = host.execute( new RequestBuilder( command ) );
                if( result.hasSucceeded() ){
                    if ( result.getStdOut().contains( type.name().toLowerCase() ) ){
                        resultList.add( host );
                    }
                }
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }
        return resultList;
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
