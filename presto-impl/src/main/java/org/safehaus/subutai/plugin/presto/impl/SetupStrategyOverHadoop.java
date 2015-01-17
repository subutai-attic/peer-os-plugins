package org.safehaus.subutai.plugin.presto.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;

import com.google.common.collect.Sets;


public class SetupStrategyOverHadoop extends SetupHelper implements ClusterSetupStrategy
{
    private Environment environment;
    private Set<ContainerHost> nodesToInstallPresto;


    public SetupStrategyOverHadoop( TrackerOperation po, PrestoImpl manager, PrestoClusterConfig config )
    {
        super( po, manager, config );
    }


    @Override
    public PrestoClusterConfig setup() throws ClusterSetupException
    {
        check();
        install();
        return config;
    }


    private void check() throws ClusterSetupException
    {
        po.addLog( "Checking prerequisites..." );

        String m = "Malformed configuration: ";
        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException( m + "Cluster already exists: " + config.getClusterName() );
        }
        if ( config.getCoordinatorNode() == null )
        {
            throw new ClusterSetupException( m + "Coordinator node is not specified" );
        }
        if ( CollectionUtil.isCollectionEmpty( config.getWorkers() ) )
        {
            throw new ClusterSetupException( m + "No workers nodes" );
        }

        // check Hadoop cluster
        HadoopClusterConfig hc = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }
        if ( !hc.getAllNodes().containsAll( config.getAllNodes() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }

        environment = manager.getEnvironmentManager().getEnvironmentByUUID( hc.getEnvironmentId() );

        if ( environment == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop environment" );
        }

        checkConnected( environment );
        nodesToInstallPresto = Sets.newHashSet();

        List<PrestoClusterConfig> prestoClusters = manager.getClusters();

        //check installed packages
        RequestBuilder checkInstalledCommand = manager.getCommands().getCheckInstalledCommand();
        for ( UUID uuid : config.getAllNodes() )
        {
            ContainerHost node = environment.getContainerHostById( uuid );
            if ( node == null )
            {
                throw new ClusterSetupException( String.format( "Node %s not found in environment", uuid ) );
            }

            //check if node belongs to some existing presto cluster
            for ( PrestoClusterConfig cluster : prestoClusters )
            {
                if ( cluster.getAllNodes().contains( uuid ) )
                {
                    throw new ClusterSetupException(
                            String.format( "Node %s already belongs to Presto cluster %s", node.getHostname(),
                                    cluster.getClusterName() ) );
                }
            }
            //filter nodes missing Presto
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( !result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    nodesToInstallPresto.add( node );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Error while checking Presto installation: %s; ", e.getMessage() ) );
            }
        }
    }


    private void install() throws ClusterSetupException
    {
        try
        {
            //install presto
            po.addLog( "Installing Presto..." );
            for ( ContainerHost node : nodesToInstallPresto )
            {
                CommandResult result = node.execute( manager.getCommands().getInstallCommand() );
                processResult( node, result );
            }
            po.addLog( "Configuring cluster..." );
            configureAsCoordinator( environment.getContainerHostById( config.getCoordinatorNode() ), environment );
            configureAsWorker( environment.getContainerHostsByIds( config.getWorkers() ) );
            //startNodes( environment.getContainerHostsByIds( config.getAllNodes() ) );

            po.addLog( "Saving cluster info..." );
            config.setEnvironmentId( environment.getId() );
            try
            {
                manager.saveConfig( config );
            }
            catch ( ClusterException e )
            {
                throw new ClusterSetupException( e );
            }

            po.addLog( "Installation succeeded" );

            //subscribe to alerts
            manager.subscribeToAlerts( environment );
        }
        catch ( CommandException | MonitorException e )
        {
            throw new ClusterSetupException(
                    String.format( "Error while installing Presto on container %s; ", e.getMessage() ) );
        }
    }
}
