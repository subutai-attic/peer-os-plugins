package io.subutai.plugin.presto.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;


public class SetupStrategyOverHadoop extends SetupHelper implements ClusterSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( SetupStrategyOverHadoop.class );
    private Environment environment;
    private Set<ContainerHost> nodesToInstallPresto;
    CommandUtil commandUtil;

    public SetupStrategyOverHadoop( TrackerOperation po, PrestoImpl manager, PrestoClusterConfig config )
    {
        super( po, manager, config );
        commandUtil = new CommandUtil();
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

        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + hc.getEnvironmentId().toString(), e );
            return;
        }

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
            ContainerHost node = null;
            try
            {
                node = environment.getContainerHostById( uuid );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                po.addLogFailed( "Container host not found" );
            }
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
                CommandResult result = commandUtil.execute( manager.getCommands().getInstallCommand(), node );
                checkInstalled( node, result );
            }
            po.addLog( "Configuring cluster..." );
            try
            {
                configureAsCoordinator( environment.getContainerHostById( config.getCoordinatorNode() ), environment );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                po.addLogFailed( "Container host not found" );
            }
            try
            {
                configureAsWorker( environment.getContainerHostsByIds( config.getWorkers() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                po.addLogFailed( "Container host not found" );
            }
            startNodes( environment.getContainerHostsByIds( config.getAllNodes() ) );

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
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException(
                    String.format( "Error while starting Presto on container %s; ", e.getMessage() ) );
        }
    }
}
