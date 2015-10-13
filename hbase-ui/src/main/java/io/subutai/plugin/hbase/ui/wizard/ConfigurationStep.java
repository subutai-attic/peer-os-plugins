/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.hbase.ui.wizard;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.data.util.BeanItemContainer;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.ComponentContainer;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.TwinColSelect;
import com.vaadin.ui.VerticalLayout;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hbase.api.HBaseConfig;


public class ConfigurationStep extends Panel
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigurationStep.class );
    private final Hadoop hadoop;
    private final Wizard wizard;


    public ConfigurationStep( final Hadoop hadoop, final Wizard wizard )
    {

        this.hadoop = hadoop;
        this.wizard = wizard;

        setSizeFull();

        GridLayout content = new GridLayout( 1, 4 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        TextField nameTxt = new TextField( "Cluster name" );
        nameTxt.setId( "hbaseClusterName" );
        nameTxt.setRequired( true );
        nameTxt.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent e )
            {
                wizard.getConfig().setClusterName( e.getProperty().getValue().toString().trim() );
            }
        } );
        nameTxt.setValue( wizard.getConfig().getClusterName() );

        Button next = new Button( "Next" );
        next.setId( "HbaseNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                nextClickHandler( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "HbaseBack" );
        back.addStyleName( "default" );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                wizard.back();
            }
        } );

        VerticalLayout layout = new VerticalLayout();
        layout.setSpacing( true );
        layout.addComponent( new Label( "Please, specify installation settings" ) );
        layout.addComponent( content );

        HorizontalLayout buttons = new HorizontalLayout();
        buttons.addComponent( back );
        buttons.addComponent( next );

        content.addComponent( nameTxt );
        addOverHadoopComponents( content, wizard.getConfig() );

        content.addComponent( buttons );

        setContent( layout );
    }


    private void addOverHadoopComponents( ComponentContainer parent, final HBaseConfig config )
    {
        final ComboBox hadoopClustersCombo = new ComboBox( "Hadoop cluster" );
        final ComboBox masterNodeCombo = new ComboBox( "Master node" );
        final TwinColSelect regionServers =
                new TwinColSelect( "Region Servers", new ArrayList<EnvironmentContainerHost>() );
        final TwinColSelect quorumPeers =
                new TwinColSelect( "Quroum Peers", new ArrayList<EnvironmentContainerHost>() );
        final TwinColSelect backUpMasters =
                new TwinColSelect( "Backup Masters", new ArrayList<EnvironmentContainerHost>() );

        hadoopClustersCombo.setId( "HbaseConfHadoopCluster" );
        masterNodeCombo.setId( "HbaseMasters" );
        regionServers.setId( "HbaseRegions" );
        quorumPeers.setId( "HbaseQuorums" );
        backUpMasters.setId( "HbaseBackupMasters" );

        masterNodeCombo.setImmediate( true );
        masterNodeCombo.setTextInputAllowed( false );
        masterNodeCombo.setRequired( true );
        masterNodeCombo.setNullSelectionAllowed( false );

        hadoopClustersCombo.setImmediate( true );
        hadoopClustersCombo.setTextInputAllowed( false );
        hadoopClustersCombo.setRequired( true );
        hadoopClustersCombo.setNullSelectionAllowed( true );

        regionServers.setItemCaptionPropertyId( "hostname" );
        regionServers.setRows( 7 );
        regionServers.setMultiSelect( true );
        regionServers.setImmediate( true );
        regionServers.setLeftColumnCaption( "Available Nodes" );
        regionServers.setRightColumnCaption( "Selected Nodes" );
        regionServers.setWidth( 100, Unit.PERCENTAGE );
        regionServers.setRequired( true );


        quorumPeers.setItemCaptionPropertyId( "hostname" );
        quorumPeers.setRows( 7 );
        quorumPeers.setMultiSelect( true );
        quorumPeers.setImmediate( true );
        quorumPeers.setLeftColumnCaption( "Available Nodes" );
        quorumPeers.setRightColumnCaption( "Selected Nodes" );
        quorumPeers.setWidth( 100, Unit.PERCENTAGE );
        quorumPeers.setRequired( true );


        backUpMasters.setItemCaptionPropertyId( "hostname" );
        backUpMasters.setRows( 7 );
        backUpMasters.setMultiSelect( true );
        backUpMasters.setImmediate( true );
        backUpMasters.setLeftColumnCaption( "Available Nodes" );
        backUpMasters.setRightColumnCaption( "Selected Nodes" );
        backUpMasters.setWidth( 100, Unit.PERCENTAGE );
        backUpMasters.setRequired( true );


        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        if ( clusters.size() > 0 )
        {
            for ( HadoopClusterConfig hadoopClusterInfo : clusters )
            {
                hadoopClustersCombo.addItem( hadoopClusterInfo );
                hadoopClustersCombo.setItemCaption( hadoopClusterInfo, hadoopClusterInfo.getClusterName() );
            }
        }

        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            if ( clusters.size() > 0 )
            {
                hadoopClustersCombo.setValue( clusters.iterator().next() );
            }
        }
        else
        {
            HadoopClusterConfig info = hadoop.getCluster( config.getClusterName() );
            if ( info != null )
            //restore cluster
            {
                hadoopClustersCombo.setValue( info );
            }
            else if ( clusters.size() > 0 )
            {
                hadoopClustersCombo.setValue( clusters.iterator().next() );
            }
        }

        if ( hadoopClustersCombo.getValue() != null )
        {
            HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) hadoopClustersCombo.getValue();


            config.setHadoopClusterName( hadoopInfo.getClusterName() );
            config.setHadoopNameNode( hadoopInfo.getNameNode() );

            /** fill all tables  */


            Set<EnvironmentContainerHost> hadoopHosts = getHadoopContainerHosts( hadoopInfo );
            config.setEnvironmentId( hadoopInfo.getEnvironmentId() );
            regionServers
                    .setContainerDataSource( new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );
            quorumPeers
                    .setContainerDataSource( new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );
            backUpMasters
                    .setContainerDataSource( new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );
            for ( EnvironmentContainerHost host : hadoopHosts )
            {
                masterNodeCombo.addItem( host );
                masterNodeCombo.setItemCaption( host, host.getHostname() );
            }
        }


        hadoopClustersCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) event.getProperty().getValue();
                    try
                    {

                        wizard.getEnvironmentManager().loadEnvironment( hadoopInfo.getEnvironmentId() );
                        Set<EnvironmentContainerHost> hadoopHosts = getHadoopContainerHosts( hadoopInfo );
                        regionServers.setValue( null );
                        regionServers.setContainerDataSource(
                                new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );

                        quorumPeers.setValue( null );
                        quorumPeers.setContainerDataSource(
                                new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );

                        backUpMasters.setValue( null );
                        backUpMasters.setContainerDataSource(
                                new BeanItemContainer<>( EnvironmentContainerHost.class, hadoopHosts ) );

                        masterNodeCombo.setValue( null );
                        masterNodeCombo.removeAllItems();
                        for ( EnvironmentContainerHost host : hadoopHosts )
                        {
                            masterNodeCombo.addItem( host );
                            masterNodeCombo.setItemCaption( host, host.getHostname() );
                        }
                        config.setHadoopClusterName( hadoopInfo.getClusterName() );
                        config.setRegionServers( new HashSet<String>() );
                        config.setQuorumPeers( new HashSet<String>() );
                        config.setBackupMasters( new HashSet<String>() );
                        config.setHbaseMaster( null );
                    }
                    catch ( EnvironmentNotFoundException e )
                    {
                        LOGGER.error( "Environment not found.", e );
                    }
                }
                else
                {
                    regionServers.removeAllItems();
                    quorumPeers.removeAllItems();
                    backUpMasters.removeAllItems();
                    masterNodeCombo.removeAllItems();
                }
            }
        } );

        masterNodeCombo.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    EnvironmentContainerHost master = ( EnvironmentContainerHost ) event.getProperty().getValue();
                    config.setHbaseMaster( master.getId() );


                    HadoopClusterConfig hadoopInfo = ( HadoopClusterConfig ) hadoopClustersCombo.getValue();
                    if ( config.getBackupMasters() != null && !config.getBackupMasters().isEmpty() )
                    {
                        config.getBackupMasters().remove( master.getId() );
                    }

                    //                    List<UUID> hadoopNodes = hadoopInfo.getAllNodes();
                    Set<EnvironmentContainerHost> hadoopHosts = getHadoopContainerHosts( hadoopInfo );
                    hadoopHosts.remove( master );


                    /** fill region servers table */
                    regionServers.getContainerDataSource().removeAllItems();
                    for ( EnvironmentContainerHost host : hadoopHosts )
                    {
                        regionServers.getContainerDataSource().addItem( host );
                    }

                    Collection ls = regionServers.getListeners( Property.ValueChangeListener.class );
                    Property.ValueChangeListener h =
                            ls.isEmpty() ? null : ( Property.ValueChangeListener ) ls.iterator().next();
                    if ( h != null )
                    {
                        regionServers.removeValueChangeListener( h );
                    }
                    regionServers.setValue( config.getRegionServers() );
                    if ( h != null )
                    {
                        regionServers.addValueChangeListener( h );
                    }

                    /** fill quorum peers servers table */
                    quorumPeers.getContainerDataSource().removeAllItems();
                    for ( EnvironmentContainerHost host : hadoopHosts )
                    {
                        quorumPeers.getContainerDataSource().addItem( host );
                    }

                    ls = quorumPeers.getListeners( Property.ValueChangeListener.class );
                    h = ls.isEmpty() ? null : ( Property.ValueChangeListener ) ls.iterator().next();
                    if ( h != null )
                    {
                        quorumPeers.removeValueChangeListener( h );
                    }
                    quorumPeers.setValue( config.getQuorumPeers() );
                    if ( h != null )
                    {
                        quorumPeers.addValueChangeListener( h );
                    }

                    /** fill back up master servers table */
                    backUpMasters.getContainerDataSource().removeAllItems();
                    for ( EnvironmentContainerHost agent : hadoopHosts )
                    {
                        backUpMasters.getContainerDataSource().addItem( agent );
                    }

                    ls = backUpMasters.getListeners( Property.ValueChangeListener.class );
                    h = ls.isEmpty() ? null : ( Property.ValueChangeListener ) ls.iterator().next();
                    if ( h != null )
                    {
                        backUpMasters.removeValueChangeListener( h );
                    }
                    backUpMasters.setValue( config.getBackupMasters() );
                    if ( h != null )
                    {
                        backUpMasters.addValueChangeListener( h );
                    }
                }
            }
        } );

        if ( config.getHbaseMaster() != null )
        {
            masterNodeCombo.setValue( config.getHbaseMaster() );
        }

        if ( !CollectionUtil.isCollectionEmpty( config.getRegionServers() ) )
        {
            regionServers.setValue( config.getRegionServers() );
        }

        if ( !CollectionUtil.isCollectionEmpty( config.getQuorumPeers() ) )
        {
            regionServers.setValue( config.getQuorumPeers() );
        }

        if ( !CollectionUtil.isCollectionEmpty( config.getBackupMasters() ) )
        {
            regionServers.setValue( config.getBackupMasters() );
        }

        regionServers.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<EnvironmentContainerHost> hosts =
                            new HashSet<>( ( Collection<EnvironmentContainerHost> ) event.getProperty().getValue() );
                    Set<String> hostIds = new HashSet<>();
                    for ( EnvironmentContainerHost host : hosts )
                    {
                        hostIds.add( host.getId() );
                    }
                    config.setRegionServers( hostIds );
                }
            }
        } );

        quorumPeers.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<EnvironmentContainerHost> hosts =
                            new HashSet<>( ( Collection<EnvironmentContainerHost> ) event.getProperty().getValue() );
                    Set<String> hostIds = new HashSet<>();
                    for ( EnvironmentContainerHost host : hosts )
                    {
                        hostIds.add( host.getId() );
                    }
                    config.setQuorumPeers( hostIds );
                }
            }
        } );


        backUpMasters.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    Set<EnvironmentContainerHost> hosts =
                            new HashSet<>( ( Collection<EnvironmentContainerHost> ) event.getProperty().getValue() );
                    Set<String> hostIds = new HashSet<>();
                    for ( EnvironmentContainerHost host : hosts )
                    {
                        hostIds.add( host.getId() );
                    }
                    config.setBackupMasters( hostIds );
                }
            }
        } );

        parent.addComponent( hadoopClustersCombo );
        parent.addComponent( masterNodeCombo );
        parent.addComponent( regionServers );
        parent.addComponent( quorumPeers );
        parent.addComponent( backUpMasters );
    }


    private Set<EnvironmentContainerHost> getHadoopContainerHosts( HadoopClusterConfig hadoopInfo )
    {
        Environment hadoopEnvironment;
        try
        {
            hadoopEnvironment = wizard.getEnvironmentManager().loadEnvironment( hadoopInfo.getEnvironmentId() );
            Set<EnvironmentContainerHost> hadoopHosts = new HashSet<>();
            for ( EnvironmentContainerHost host : hadoopEnvironment.getContainerHosts() )
            {
                if ( filterNodes( hadoopInfo.getAllNodes() ).contains( host.getId() ) )
                {
                    hadoopHosts.add( host );
                }
            }
            return hadoopHosts;
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return Collections.emptySet();
    }


    //exclude hadoop nodes that are already in another flume cluster
    private List<String> filterNodes( List<String> hadoopNodes )
    {
        List<String> hbaseNodes = new ArrayList<>();
        List<String> filteredNodes = new ArrayList<>();
        for ( HBaseConfig hBaseConfig : wizard.getHbase().getClusters() )
        {
            hbaseNodes.addAll( hBaseConfig.getAllNodes() );
        }
        for ( String node : hadoopNodes )
        {
            if ( !hbaseNodes.contains( node ) )
            {
                filteredNodes.add( node );
            }
        }
        return filteredNodes;
    }


    private void nextClickHandler( Wizard wizard )
    {
        HBaseConfig config = wizard.getConfig();
        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            show( "Enter cluster name" );
            return;
        }

        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            show( "Please, select Hadoop cluster" );
        }
        else if ( config.getHbaseMaster() == null )
        {
            show( "Please, select master node" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getRegionServers() ) )
        {
            show( "Please, select nodes for region servers" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getQuorumPeers() ) )
        {
            show( "Please, select nodes for quorum peers" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getBackupMasters() ) )
        {
            show( "Please, select for back up masters" );
        }
        else
        {
            wizard.next();
        }
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
