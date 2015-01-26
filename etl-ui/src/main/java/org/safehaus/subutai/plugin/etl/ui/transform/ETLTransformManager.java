package org.safehaus.subutai.plugin.etl.ui.transform;


import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.etl.ui.SqoopComponent;
import org.safehaus.subutai.plugin.etl.ui.UIUtil;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class ETLTransformManager
{
    private final GridLayout contentRoot;
    private final ETL sqoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private final SqoopComponent sqoopComponent;

    private ETLConfig config;
    private Environment environment;
    private Hadoop hadoop;

    private int NODE_SELECTION_GRID_LAYOUT_COL_INDEX = 0;
    private int NODE_SELECTION_GRID_LAYOUT_ROW_INDEX = 2;

    QueryType type;


    public ETLTransformManager( ExecutorService executorService, ETL sqoop, Hadoop hadoop, Tracker tracker,
                                EnvironmentManager environmentManager, SqoopComponent sqoopComponent )
            throws NamingException
    {
        this.executorService = executorService;
        this.sqoopComponent = sqoopComponent;
        this.sqoop = sqoop;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 20 );
        contentRoot.setColumns( 5 );

        init( contentRoot, type );
    }


    public void init( final GridLayout gridLayout, QueryType type ){
        gridLayout.removeAllComponents();

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );

        Label clusterNameLabel = new Label( "Select Sqoop installation:" );
        controlsContent.addComponent( clusterNameLabel );

        ComboBox hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setNullSelectionAllowed( false );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        contentRoot.addComponent( hadoopClustersCombo, 0, 1 );

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

        switch ( type ){
            case HIVE:
                tabsheet.setSelectedTab( tab1 );
                final ComboBox hiveSelection = new ComboBox( "Select Hive Installation" );
                hiveSelection.setNullSelectionAllowed( false );
                hiveSelection.setImmediate( true );
                hiveSelection.setTextInputAllowed( false );
                hiveSelection.setRequired( true );
                contentRoot.addComponent( hiveSelection, NODE_SELECTION_GRID_LAYOUT_COL_INDEX, NODE_SELECTION_GRID_LAYOUT_ROW_INDEX );
                break;
            case PIG:
                tabsheet.setSelectedTab( tab2 );
                final ComboBox pigSelection = new ComboBox( "Select Pig Installation" );
                pigSelection.setNullSelectionAllowed( false );
                pigSelection.setImmediate( true );
                pigSelection.setTextInputAllowed( false );
                pigSelection.setRequired( true );
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

        gridLayout.addComponent( layout, 1, 0, 4, 17 );
    }

    public Component getContent()
    {
        return contentRoot;
    }
}
