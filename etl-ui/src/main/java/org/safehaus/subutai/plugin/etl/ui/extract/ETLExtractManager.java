package org.safehaus.subutai.plugin.etl.ui.extract;


import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.ui.ETLBaseManager;
import org.safehaus.subutai.plugin.etl.ui.ImportPanel;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.Sqoop;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.UI;


public class ETLExtractManager extends ETLBaseManager
{
    private final ImportPanel importPanel;

    public ETLExtractManager( final ExecutorService executorService, ETL etl, final Hadoop hadoop,
                              final Sqoop sqoop, Tracker tracker,
                              final EnvironmentManager environmentManager )
            throws NamingException
    {

        super( executorService, etl, hadoop, sqoop, tracker, environmentManager );

        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        if ( !clusters.isEmpty() )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClustersCombo.addItem( hadoopClusterInfo );
                hadoopClustersCombo.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        final ComboBox sqoopSelection = new ComboBox( "Select Sqoop Node" );
        sqoopSelection.setNullSelectionAllowed( false );
        sqoopSelection.setImmediate( true );
        sqoopSelection.setTextInputAllowed( false );
        sqoopSelection.setRequired( true );
        contentRoot.addComponent( sqoopSelection, 0, 2 );

        importPanel = new ImportPanel( etl, sqoop, executorService, tracker );
        contentRoot.addComponent( importPanel, 1, 0, 19, 17 );

        // event listeners
        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                progressIcon.setVisible( true );
                if ( event.getProperty().getValue() != null )
                {
                    final HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    sqoopSelection.setValue( null );
                    sqoopSelection.removeAllItems();
                    executorService.execute( new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
                            final Set<ContainerHost> hadoopNodes =
                                    hadoopEnvironment.getContainerHostsByIds( Sets
                                            .newHashSet( hadoopInfo.getAllNodes() ) );
                            UI.getCurrent().access(new Runnable() {
                                @Override
                                public void run() {

                                    Set<ContainerHost> filteredNodes = filterSqoopInstalledNodes( hadoopNodes );

                                    if ( filteredNodes.isEmpty() ){
                                        show( "No node has subutai Sqoop package installed" );
                                    }
                                    else {
                                        for ( ContainerHost hadoopNode : filteredNodes )
                                        {
                                            sqoopSelection.addItem( hadoopNode );
                                            sqoopSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                                        }
                                    };
                                    disableProgressBar();
                                }
                            });
                        }
                    } );
                }
            }
        } );

        sqoopSelection.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    ContainerHost containerHost = ( ContainerHost ) event.getProperty().getValue();
                    importPanel.setHost( containerHost );
                    SqoopConfig config = findSqoopConfigOfContainerHost( sqoop.getClusters(), containerHost );
                    importPanel.setClusterName( config.getClusterName() );
                }
            }
        } );
    }



}
