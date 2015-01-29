package org.safehaus.subutai.plugin.etl.ui.load;


import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.ui.ETLBaseManager;
import org.safehaus.subutai.plugin.etl.ui.ExportPanel;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.Sqoop;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

import com.vaadin.data.Property;
import com.vaadin.ui.ComboBox;


public class ETLLoadManager extends ETLBaseManager
{
    private final ExportPanel exportPanel;

    public ETLLoadManager( final ExecutorService executorService, ETL etl, final Hadoop hadoop, final Sqoop sqoop,
                           Tracker tracker, final EnvironmentManager environmentManager )
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

        final ComboBox sqoopSelection = new ComboBox( "Select Sqoop" );
        sqoopSelection.setNullSelectionAllowed( false );
        sqoopSelection.setImmediate( true );
        sqoopSelection.setTextInputAllowed( false );
        sqoopSelection.setRequired( true );
        contentRoot.addComponent( sqoopSelection, 0, 2 );

        exportPanel = new ExportPanel( etl, sqoop, executorService, tracker );
        contentRoot.addComponent( exportPanel, 1, 0, 19, 17 );

        // event listeners
        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    final Set<ContainerHost> hadoopNodes = environmentManager.getEnvironmentByUUID(
                            hadoopInfo.getEnvironmentId() ).getContainerHosts();

                    sqoopSelection.setValue( null );
                    sqoopSelection.removeAllItems();
                    FilterThread filterThread = new FilterThread( hadoopInfo, sqoopSelection );
                    executorService.execute( filterThread );
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
                    exportPanel.setHost( containerHost );
                    SqoopConfig config = findSqoopConfigOfContainerHost( sqoop.getClusters(), containerHost );
                    exportPanel.setClusterName( config.getClusterName() );
                }
            }
        } );
    }
}
