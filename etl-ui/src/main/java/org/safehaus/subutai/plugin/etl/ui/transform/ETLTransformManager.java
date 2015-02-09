package org.safehaus.subutai.plugin.etl.ui.transform;


import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.ui.ETLBaseManager;
import org.safehaus.subutai.plugin.etl.ui.UIUtil;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hive.api.Hive;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.safehaus.subutai.plugin.pig.api.Pig;
import org.safehaus.subutai.plugin.pig.api.PigConfig;
import org.safehaus.subutai.plugin.sqoop.api.Sqoop;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class ETLTransformManager extends ETLBaseManager
{
    private QueryPanel queryPanel;
    private Hive hive;
    private Pig pig;

    public ETLTransformManager( ExecutorService executorService, ETL etl, Hadoop hadoop, Sqoop sqoop, Tracker tracker,
                                Hive hive, Pig pig, EnvironmentManager environmentManager )
            throws NamingException
    {
        super( executorService, etl, hadoop, sqoop, tracker, environmentManager );
        this.hive = hive;
        this.pig = pig;

        init( contentRoot, type );
    }


    public void init( final GridLayout gridLayout, QueryType type ){

        gridLayout.removeAllComponents();

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

        final VerticalLayout tab1 = new VerticalLayout();
        tab1.setCaption( QueryType.HIVE.name() );
        tabsheet.addTab(tab1);

        final VerticalLayout tab2 = new VerticalLayout();
        tab2.setCaption( QueryType.PIG.name() );
        tabsheet.addTab(tab2);

        gridLayout.addComponent( tabsheet, 1, 0, 19, 0 );

        queryPanel = new QueryPanel( this );
        gridLayout.addComponent( queryPanel, 1, 1, 19, 19 );

        if ( type == null ){
            type = QueryType.HIVE;
        }

        final ComboBox hiveSelection = new ComboBox( HIVE_COMBO_BOX_CAPTION );
        hiveSelection.setImmediate( true );
        hiveSelection.setTextInputAllowed( false );
        hiveSelection.setRequired( true );

        final ComboBox pigSelection = new ComboBox( PIG_COMBO_BOX_CAPTION );
        pigSelection.setImmediate( true );
        pigSelection.setTextInputAllowed( false );
        pigSelection.setRequired( true );

        switch ( type ){
            case HIVE:
                hadoopClustersCombo.setValue( null );
                gridLayout.addComponent( hiveSelection, 0, 2 );
                tabsheet.setSelectedTab( tab1 );
                break;
            case PIG:
                hadoopClustersCombo.setValue( null );
                gridLayout.addComponent( pigSelection, 0, 2 );
                tabsheet.setSelectedTab( tab2 );

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
                    Environment hadoopEnvironment = null;
                    try
                    {
                        hadoopEnvironment = environmentManager.findEnvironment( hadoopInfo.getEnvironmentId() );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                    Set<ContainerHost> hadoopNodes = null;
                    if ( hadoopEnvironment != null )
                    {
                        try
                        {
                            hadoopNodes =
                                    hadoopEnvironment.getContainerHostsByIds( Sets.newHashSet( hadoopInfo.getAllNodes() ) );
                        }
                        catch ( ContainerHostNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    hiveSelection.setValue( null );
                    pigSelection.setValue( null );
                    hiveSelection.removeAllItems();
                    pigSelection.removeAllItems();
                    enableProgressBar();
                    final Set<ContainerHost> finalHadoopNodes = hadoopNodes;
                    executorService.execute( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            Set<ContainerHost> filteredHadoopNodes = filterHadoopNodes( finalHadoopNodes, queryType );
                            if ( filteredHadoopNodes.isEmpty() ){
                                show( "No node has subutai " + queryType.name() + " package installed" );
                            }
                            else {
                                for ( ContainerHost hadoopNode : filterHadoopNodes( finalHadoopNodes, queryType ) )
                                {
                                    hiveSelection.addItem( hadoopNode );
                                    hiveSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );

                                    pigSelection.addItem( hadoopNode );
                                    pigSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                                }
                            }
                            disableProgressBar();
                        }
                    } );
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
                    HiveConfig config = findHiveConfigOfContainerHost( hive.getClusters(), containerHost );
                    queryPanel.setContainerHost( containerHost );
                    if ( config != null ){
                        queryPanel.setClusterName( config.getClusterName() );
                    }
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
                    PigConfig config = findPigConfigOfContainerHost( pig.getClusters(), containerHost );
                    queryPanel.setContainerHost( containerHost );
                    if ( config != null ){
                        queryPanel.setClusterName( config.getClusterName() );
                    }
                }
            }
        } );
    }
}
