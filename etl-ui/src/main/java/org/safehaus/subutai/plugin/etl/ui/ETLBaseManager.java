package org.safehaus.subutai.plugin.etl.ui;


import java.util.HashSet;
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
import org.safehaus.subutai.plugin.etl.ui.transform.QueryType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;

import com.google.common.collect.Sets;
import com.vaadin.server.ThemeResource;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.Component;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.UI;


public class ETLBaseManager
{
    public final GridLayout contentRoot;
    public final ETL sqoop;
    public final ExecutorService executorService;
    public final Tracker tracker;
    public final EnvironmentManager environmentManager;

    public  Hadoop hadoop;
    public final Embedded progressIcon = new Embedded( "", new ThemeResource( "img/spinner.gif" ) );

    public QueryType type;
    public HorizontalLayout hadoopComboWithProgressIcon;
    public ComboBox hadoopClustersCombo;

    public ETLBaseManager(  ExecutorService executorService, ETL sqoop, Hadoop hadoop, Tracker tracker,
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

        hadoopComboWithProgressIcon = new HorizontalLayout();
        hadoopComboWithProgressIcon.setSpacing( true );

        hadoopClustersCombo = new ComboBox( "Select Hadoop Cluster" );
        hadoopClustersCombo.setNullSelectionAllowed( false );
        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        hadoopComboWithProgressIcon.addComponent( hadoopClustersCombo );


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


    public class FilterThread extends Thread {
        HadoopClusterConfig hadoopInfo;
        ComboBox comboBox;
        Set<ContainerHost> resultList = new HashSet<>();

        public FilterThread( HadoopClusterConfig hadoopInfo, ComboBox comboBox ){
            this.hadoopInfo = hadoopInfo;
            this.comboBox = comboBox;
        }

        @Override
        public void run() {
            Environment hadoopEnvironment = environmentManager.getEnvironmentByUUID( hadoopInfo.getEnvironmentId() );
            final Set<ContainerHost> hadoopNodes =
                    hadoopEnvironment.getContainerHostsByIds( Sets.newHashSet( hadoopInfo.getAllNodes() ) );
            UI.getCurrent().access(new Runnable() {
                @Override
                public void run() {
                    enableProgressBar();
                    Set<ContainerHost> filteredNodes = filterSqoopInstalledNodes( hadoopNodes );

                    if ( filteredNodes.isEmpty() ){
                        show( "No node has subutai Sqoop package installed" );
                    }
                    else {
                        for ( ContainerHost hadoopNode : filteredNodes )
                        {
                            comboBox.addItem( hadoopNode );
                            comboBox.setItemCaption( hadoopNode, hadoopNode.getHostname() );
                        }
                    };
                    resultList = filteredNodes;
                    disableProgressBar();
                }
            });
        }
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
}

