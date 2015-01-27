package org.safehaus.subutai.plugin.etl.ui.load;


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
import org.safehaus.subutai.plugin.etl.ui.ExportPanel;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.server.ThemeResource;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;


public class ETLLoadManager
{
    private final GridLayout contentRoot;
    private final ExportPanel exportPanel;
    private final ETL sqoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;

    private ETLConfig config;
    private Environment environment;
    private Hadoop hadoop;
    public final Embedded PROGRESS_ICON = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
    HorizontalLayout hadoopComboWithProgressIcon;


    public ETLLoadManager( ExecutorService executorService, ETL etl, Hadoop hadoop, Tracker tracker,
                           final EnvironmentManager environmentManager )
            throws NamingException
    {
        this.executorService = executorService;
        this.sqoop = etl;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 20 );
        contentRoot.setColumns( 5 );

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );

        Label clusterNameLabel = new Label( "Select Sqoop installation:" );
        controlsContent.addComponent( clusterNameLabel );

        contentRoot.addComponent( controlsContent, 0, 0 );

        hadoopComboWithProgressIcon = new HorizontalLayout();
        hadoopComboWithProgressIcon.setSpacing( true );

        ComboBox hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setNullSelectionAllowed( false );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );

        hadoopComboWithProgressIcon.addComponent( hadoopClustersCombo );

        hadoopComboWithProgressIcon.addComponent( PROGRESS_ICON );
        hadoopComboWithProgressIcon.setComponentAlignment( PROGRESS_ICON, Alignment.BOTTOM_CENTER );
        PROGRESS_ICON.setVisible( false );
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

        final ComboBox sqoopSelection = new ComboBox( "Select Sqoop" );
        sqoopSelection.setNullSelectionAllowed( false );
        sqoopSelection.setImmediate( true );
        sqoopSelection.setTextInputAllowed( false );
        sqoopSelection.setRequired( true );
        contentRoot.addComponent( sqoopSelection, 0, 2 );

        exportPanel = new ExportPanel( etl, executorService, tracker );
        contentRoot.addComponent( exportPanel, 1, 0, 4, 17 );


        // event listeners
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

                    sqoopSelection.setValue( null );
                    sqoopSelection.removeAllItems();
                    enableProgressBar();
                    Set<ContainerHost> filteredHadoopNodes = filterSqoopInstalledNodes( hadoopNodes );
                    if ( filteredHadoopNodes.isEmpty() ){
                        show( "No node has subutai Sqoop package installed" );
                    }
                    else {
                        for ( ContainerHost hadoopNode : filterSqoopInstalledNodes( hadoopNodes ) )
                        {
                            sqoopSelection.addItem( hadoopNode );
                            sqoopSelection.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                        }
                    }
                    disableProgressBar();
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
                }
            }
        } );
    }

    private Set<ContainerHost> filterSqoopInstalledNodes( Set<ContainerHost> containerHosts ){
        Set<ContainerHost> resultList = new HashSet<>();
        for( ContainerHost host : containerHosts ){
            String command = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            try
            {
                CommandResult result = host.execute( new RequestBuilder( command ) );
                if( result.hasSucceeded() ){
                    if ( result.getStdOut().contains( "sqoop" ) ){
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


    public synchronized void enableProgressBar()
    {
        PROGRESS_ICON.setVisible( true );
    }


    public synchronized void disableProgressBar()
    {
        PROGRESS_ICON.setVisible( false );
    }


    public void show( String message ){
        Notification.show( message );
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
