package org.safehaus.subutai.plugin.etl.ui.extract;


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
import org.safehaus.subutai.plugin.etl.ui.ImportPanel;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

import com.google.common.collect.Sets;
import com.vaadin.data.Property;
import com.vaadin.server.ThemeResource;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;


public class ETLExtractManager
{
    private final GridLayout contentRoot;
    private final ImportPanel importPanel;
    private final ETL etl;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;

    private ETLConfig config;
    private Environment environment;
    private Hadoop hadoop;
    public Embedded progressIcon;
    HorizontalLayout hadoopComboWithProgressIcon;


    public ETLExtractManager( ExecutorService executorService, ETL etl, final Hadoop hadoop, Tracker tracker,
                              final EnvironmentManager environmentManager )
            throws NamingException
    {

        this.executorService = executorService;
        this.etl = etl;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;


        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 20 );
        contentRoot.setColumns( 20 );

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );

        hadoopComboWithProgressIcon = new HorizontalLayout();
        hadoopComboWithProgressIcon.setSpacing( true );

        ComboBox hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setNullSelectionAllowed( false );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );

        hadoopComboWithProgressIcon.addComponent( hadoopClustersCombo );

        progressIcon = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );
        hadoopComboWithProgressIcon.addComponent( progressIcon );
        hadoopComboWithProgressIcon.setComponentAlignment( progressIcon, Alignment.BOTTOM_CENTER );
        contentRoot.addComponent( hadoopComboWithProgressIcon, 0, 1 );

        disableProgressBar();

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

        importPanel = new ImportPanel( etl, executorService, tracker );
        contentRoot.addComponent( importPanel, 1, 0, 19, 17 );


        // event listeners
        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    enableProgressBar();
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
                    Set<ContainerHost> hadoopNodes =
                            hadoopEnvironment.getContainerHostsByIds( Sets.newHashSet( hadoopInfo.getAllNodes() ) );

                    sqoopSelection.setValue( null );
                    sqoopSelection.removeAllItems();
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
                    importPanel.setHost( containerHost );
                }
            }
        } );

        contentRoot.addComponent( controlsContent, 0, 0 );
    }


    private Set<ContainerHost> filterSqoopInstalledNodes( Set<ContainerHost> containerHosts ){
        Set<ContainerHost> resultList = new HashSet<>();
        for( ContainerHost host : containerHosts ){
            String command = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            try
            {
                CommandResult result = host.execute( new RequestBuilder( command ) );
                if( result.hasSucceeded() ){
                    if ( result.getStdOut().contains( SqoopConfig.PRODUCT_KEY.toLowerCase() ) ){
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


    public void enableProgressBar()
    {
        progressIcon.setVisible( true );
    }


    public void disableProgressBar()
    {
        progressIcon.setVisible( false );
    }

    public void show( String message ){
        Notification.show( message );
    }


    public Component getContent()
    {
        return contentRoot;
    }
}
