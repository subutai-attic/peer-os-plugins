package org.safehaus.subutai.plugin.hive.ui.wizard;


import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hive.api.Hive;
import org.safehaus.subutai.plugin.hive.api.HiveConfig;
import org.safehaus.subutai.server.ui.api.PortalModuleService;

import com.google.common.base.Strings;
import com.vaadin.data.Property;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.ComponentContainer;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;


public class NodeSelectionStep extends Panel
{

    private final Hive hive;
    private final Hadoop hadoop;
    private int controlWidth = 350;
    private EnvironmentManager environmentManager;
    private PortalModuleService portalModuleService;
    private HadoopClusterConfig hc;
    private ContainerHost selected;


    public NodeSelectionStep( final Hive hive, final Hadoop hadoop, final EnvironmentManager environmentManager,
                              final Wizard wizard, final PortalModuleService portalModuleService )
    {

        this.hive = hive;
        this.hadoop = hadoop;
        this.environmentManager = environmentManager;
        this.portalModuleService = portalModuleService;

        setSizeFull();

        final GridLayout content = new GridLayout( 1, 2 );
        content.setSizeFull();
        content.setSpacing( true );
        content.setMargin( true );

        TextField nameTxt = new TextField( "Cluster name" );
        nameTxt.setId( "HiveClusterName" );
        nameTxt.setRequired( true );
        nameTxt.setWidth( controlWidth, Unit.POINTS );
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
        next.setId( "HiveNodeSelectionNext" );
        next.addStyleName( "default" );
        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                nextButtonClickHandler( wizard );
            }
        } );

        Button back = new Button( "Back" );
        back.setId( "HiveNodeSelectionBack" );
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

        if ( !addSettingsControls( content, wizard.getConfig() ) )
        {
            wizard.back();
        }

        content.addComponent( buttons );

        setContent( layout );
    }


    private boolean addSettingsControls( ComponentContainer parent, final HiveConfig config )
    {
        ComboBox hadoopClusters = new ComboBox( "Hadoop cluster" );
        hadoopClusters.setId( "HiveHadoopClusterCb" );
        final ComboBox cmbServerNode = makeServerNodeComboBox( config );

        hadoopClusters.setImmediate( true );
        hadoopClusters.setWidth( controlWidth, Unit.POINTS );
        hadoopClusters.setTextInputAllowed( false );
        hadoopClusters.setRequired( true );
        hadoopClusters.setNullSelectionAllowed( false );
        hadoopClusters.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( event.getProperty().getValue() != null )
                {
                    hc = ( HadoopClusterConfig ) event.getProperty().getValue();
                    config.setHadoopClusterName( hc.getClusterName() );

//                    ContainerHost selected;
                    if ( config.getServer() != null )
                    {
                        try
                        {
                            selected = environmentManager.findEnvironment( config.getEnvironmentId() )
                                                         .getContainerHostById( config.getServer() );
                        }
                        catch ( ContainerHostNotFoundException |EnvironmentNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    else
                    {
                        try
                        {
                            selected = environmentManager.findEnvironment( hc.getEnvironmentId() )
                                                         .getContainerHostById( hc.getNameNode() );
                        }
                        catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                        {
                            e.printStackTrace();
                        }
                    }
                    fillServerNodeComboBox( cmbServerNode, hc, selected );
                    filterNodes( cmbServerNode, hc );
                    // TODO if all nodes are filtered, then notify user
                }
            }
        } );

        List<HadoopClusterConfig> clusters = hadoop.getClusters();
        if ( !clusters.isEmpty() )
        {
            for ( HadoopClusterConfig hci : clusters )
            {
                hadoopClusters.addItem( hci );
                hadoopClusters.setItemCaption( hci, hci.getClusterName() );
            }
        }
        else if ( portalModuleService != null )
        {

            portalModuleService.loadDependentModule( HadoopClusterConfig.PRODUCT_KEY );
            return false;
        }

        String hn = config.getHadoopClusterName();
        if ( !Strings.isNullOrEmpty( hn ) )
        {
            HadoopClusterConfig info = hadoop.getCluster( hn );
            if ( info != null )
            {
                hadoopClusters.setValue( info );
            }
        }

        parent.addComponent( hadoopClusters );
        parent.addComponent( cmbServerNode );
        return true;
    }


    private ComboBox makeServerNodeComboBox( final HiveConfig config )
    {
        ComboBox cb = new ComboBox( "Server node" );
        cb.setId( "HiveserverNodecb" );
        cb.setImmediate( true );
        cb.setTextInputAllowed( false );
        cb.setRequired( true );
        cb.setNullSelectionAllowed( false );
        cb.setWidth( controlWidth, Unit.POINTS );
        cb.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                if ( hc != null )
                {
                    UUID hiveMaster = ( UUID ) event.getProperty().getValue();
                    config.setServer( hiveMaster );
                    config.getClients().clear();
                    config.getClients().addAll( hc.getAllNodes() );
                    config.getClients().remove( hiveMaster );
                }
            }
        } );
        return cb;
    }


    private void fillServerNodeComboBox( ComboBox serverNode, HadoopClusterConfig hadoopInfo, ContainerHost selected )
    {
        serverNode.removeAllItems();
        List<UUID> slaves = hadoopInfo.getAllSlaveNodes();
        for ( UUID uuid : hadoopInfo.getAllNodes() )
        {
            serverNode.addItem( getHost( hadoopInfo, uuid ).getId() );
            // TODO
            String caption = getHost( hadoopInfo, uuid ).getHostname();
            if ( hadoopInfo.getJobTracker().equals( uuid ) )
            {
                caption += " [JobTracker]";
            }
            else if ( hadoopInfo.getNameNode().equals( uuid ) )
            {
                caption += " [NameNode]";
            }
            else if ( hadoopInfo.getSecondaryNameNode().equals( uuid ) )
            {
                caption += " [SecondaryNameNode]";
            }
            else if ( slaves.contains( uuid ) )
            {
                caption += " [Slave Node]";
            }
            serverNode.setItemCaption( getHost( hadoopInfo, uuid ).getId(), caption );
        }
        if ( selected != null )
        {
            serverNode.setValue( selected.getId() );
        }
    }


    private ContainerHost getHost( HadoopClusterConfig config, UUID uuid )
    {
        try
        {
            return environmentManager.findEnvironment( config.getEnvironmentId() ).getContainerHostById( uuid );
        }
        catch ( ContainerHostNotFoundException  | EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }


    private void filterNodes( final ComboBox serverNode, final HadoopClusterConfig hadoopClusterConfig )
    {
        Collection<UUID> items = ( Collection<UUID> ) serverNode.getItemIds();
        final Set<UUID> set = new HashSet<>( items );
        for ( final UUID uuid : set )
        {
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    ContainerHost host = null;
                    try
                    {
                        host = environmentManager.findEnvironment( hadoopClusterConfig.getEnvironmentId() )
                                          .getContainerHostById( uuid );
                    }
                    catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                    {
                        e.printStackTrace();
                    }
                    boolean isInstalled = hive.isInstalled( hadoopClusterConfig.getClusterName(), host.getHostname() );
                    if ( isInstalled )
                    {
                        serverNode.removeItem( uuid );
                    }
                }
            } ).start();
        }
    }


    private void nextButtonClickHandler( Wizard wizard )
    {
        HiveConfig config = wizard.getConfig();

        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            show( "Enter name for Hive installation" );
        }
        else if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            show( "Select Hadoop cluster" );
        }
        else if ( config.getServer() == null )
        {
            show( "Select server node" );
        }
        else if ( CollectionUtil.isCollectionEmpty( config.getClients() ) )
        {
            show( "Select client nodes" );
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
