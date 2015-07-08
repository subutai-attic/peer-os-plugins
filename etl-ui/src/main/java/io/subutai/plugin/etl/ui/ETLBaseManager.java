package io.subutai.plugin.etl.ui;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.etl.api.ETL;
import io.subutai.plugin.etl.ui.transform.QueryType;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hive.api.HiveConfig;
import io.subutai.plugin.pig.api.PigConfig;
import io.subutai.plugin.sqoop.api.Sqoop;
import io.subutai.plugin.sqoop.api.SqoopConfig;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.ProgressBar;


public class ETLBaseManager
{
    public static final String SQOOP_COMBO_BOX_CAPTION = "Select Sqoop Node";
    public static final String HIVE_COMBO_BOX_CAPTION = "Select Hive Installation";
    public static final String PIG_COMBO_BOX_CAPTION = "Select Pig Installation";

    public final GridLayout contentRoot;
    public final ETL etl;
    public final ExecutorService executorService;
    public final Tracker tracker;
    public final EnvironmentManager environmentManager;

    public Hadoop hadoop;
    public Sqoop sqoop;
    public ProgressBar progressIcon;
    public QueryType type;
    public HorizontalLayout hadoopComboWithProgressIcon;
    public ComboBox hadoopClustersCombo;

    public ETLBaseManager(  ExecutorService executorService, ETL etl, Hadoop hadoop, Sqoop sqoop,
                            Tracker tracker, EnvironmentManager environmentManager )
            throws NamingException
    {
        this.executorService = executorService;
        this.etl = etl;
        this.hadoop = hadoop;
        this.sqoop = sqoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;

        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 20 );
        contentRoot.setColumns( 20 );

        hadoopComboWithProgressIcon = new HorizontalLayout();
        hadoopComboWithProgressIcon.setSpacing( true );

        hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        hadoopComboWithProgressIcon.addComponent( hadoopClustersCombo );

        progressIcon = new ProgressBar();
        progressIcon.setId( "indicator" );
        progressIcon.setIndeterminate( true );
        progressIcon.setVisible( false );
        hadoopComboWithProgressIcon.addComponent( progressIcon );
        hadoopComboWithProgressIcon.setComponentAlignment( progressIcon, Alignment.BOTTOM_CENTER );

        contentRoot.addComponent( hadoopComboWithProgressIcon, 0, 1 );

        type = QueryType.HIVE;
        contentRoot.setCaption( this.getClass().getName() );
    }


    public void show( String message ){
        Notification.show( message );
    }


    public Set<ContainerHost> filterHadoopNodes( Set<ContainerHost> containerHosts, QueryType type ){
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


    public Set<ContainerHost> filterSqoopInstalledNodes( Set<ContainerHost> containerHosts ){
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


    public Component getContent()
    {
        return contentRoot;
    }


    public Hadoop getHadoop()
    {
        return hadoop;
    }


    public void setHadoop( final Hadoop hadoop )
    {
        this.hadoop = hadoop;
    }


    public SqoopConfig findSqoopConfigOfContainerHost( List<SqoopConfig> configs, ContainerHost host ){
        for ( SqoopConfig config : configs )
        {
            HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
            Environment environment = null;
            try
            {
                environment = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( containerHost.getId().equals( host.getId() ) )
                {
                    return config;
                }
            }
        }
        return null;
    }


    public HiveConfig findHiveConfigOfContainerHost( List<HiveConfig> configs, ContainerHost host ){
        for ( HiveConfig config : configs )
        {
            HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
            Environment environment = null;
            try
            {
                environment = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( containerHost.getId().equals( host.getId() ) )
                {
                    return config;
                }
            }
        }
        return null;
    }


    public PigConfig findPigConfigOfContainerHost( List<PigConfig> configs, ContainerHost host ){
        for ( PigConfig config : configs )
        {
            HadoopClusterConfig hadoopClusterConfig = hadoop.getCluster( config.getHadoopClusterName() );
            Environment environment = null;
            try
            {
                environment = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( containerHost.getId().equals( host.getId() ) )
                {
                    return config;
                }
            }
        }
        return null;
    }
}

