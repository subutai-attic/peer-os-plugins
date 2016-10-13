package io.subutai.plugin.accumulo.impl;


import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class AccumuloOverZkNHadoopSetup implements ClusterSetupStrategy
{
    final TrackerOperation po;
    final AccumuloImpl manager;
    final AccumuloClusterConfig config;
    private Environment environment;
    private Set<EnvironmentContainerHost> nodesToInstallAccumulo;


    public AccumuloOverZkNHadoopSetup( final TrackerOperation po, final AccumuloImpl manager,
                                       final AccumuloClusterConfig config, final Environment environment )
    {
        Preconditions.checkNotNull( config, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( manager, "Manager is null" );
        Preconditions.checkNotNull( environment, "Environment is null" );

        this.po = po;
        this.manager = manager;
        this.config = config;
        this.environment = environment;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();
        return config;
    }


    private void check() throws ClusterSetupException
    {
        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException( String.format( "Cluster %s already exists", config.getClusterName() ) );
        }
        if ( config.getMaster() == null )
        {
            throw new ClusterSetupException( "Master node not specified" );
        }
        if ( CollectionUtil.isCollectionEmpty( config.getSlaves() ) )
        {
            throw new ClusterSetupException( "No slave nodes" );
        }

        EnvironmentContainerHost master;
        try
        {
            master = environment.getContainerHostById( config.getMaster() );

            if ( !master.isConnected() )
            {
                throw new ClusterSetupException( "Master is not connected" );
            }

            Set<EnvironmentContainerHost> slaves;
            slaves = environment.getContainerHostsByIds( config.getSlaves() );

            if ( slaves.size() > config.getSlaves().size() )
            {
                throw new ClusterSetupException( "Fewer slaves found in the environment than indicated" );
            }

            for ( EnvironmentContainerHost slave : slaves )
            {
                if ( !slave.isConnected() )
                {
                    throw new ClusterSetupException(
                            String.format( "Container %s is not connected", slave.getHostname() ) );
                }
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

            po.addLog( "Checking prerequisites..." );

            //gather all nodes
            final Set<EnvironmentContainerHost> allNodes = Sets.newHashSet( master );
            allNodes.addAll( slaves );

            //check if node belongs to some existing accumulo cluster
            List<AccumuloClusterConfig> accumuloClusters = manager.getClusters();
            for ( EnvironmentContainerHost node : allNodes )
            {
                for ( AccumuloClusterConfig cluster : accumuloClusters )
                {
                    if ( cluster.getAllNodes().contains( node.getId() ) )
                    {
                        throw new ClusterSetupException(
                                String.format( "Node %s already belongs to Accumulo cluster %s", node.getHostname(),
                                        cluster.getClusterName() ) );
                    }
                }
            }

            nodesToInstallAccumulo = Sets.newHashSet();

            nodesToInstallAccumulo.addAll( allNodes );

            //check hadoop installation & filter nodes needing accumulo installation
            RequestBuilder checkInstalledCommand = Commands.getCheckInstallationCommand();

            for ( final EnvironmentContainerHost node : allNodes )
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( !result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    nodesToInstallAccumulo.add( node );
                }
            }

            if ( config.getSlaves().isEmpty() )
            {
                throw new ClusterSetupException( "No slave nodes eligible for installation" );
            }
            if ( !allNodes.contains( master ) )
            {
                throw new ClusterSetupException( "Master node was omitted" );
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( "Container not found in the environment" );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
        }
    }


    private void configure() throws ClusterSetupException
    {
        if ( !nodesToInstallAccumulo.isEmpty() )
        {
            po.addLog( "Installing Accumulo..." );
            //install Accumulo
            for ( EnvironmentContainerHost node : nodesToInstallAccumulo )
            {
                try
                {
                    node.execute( Commands.getAptUpdate() );
                    CommandResult result = node.execute( Commands.getInstallCommand() );
                    checkInstalled( node, result );
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
        }

        po.addLog( "Configuring cluster..." );

        ClusterConfiguration configuration = new ClusterConfiguration( manager, po );

        try
        {
            configuration.configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            e.printStackTrace();
        }

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
    }


    private void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( Commands.getCheckInstallationCommand() );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( Commands.PACKAGE_NAME ) ) )
        {
            po.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
