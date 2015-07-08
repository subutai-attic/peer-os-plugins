package io.subutai.plugin.spark.impl;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.spark.api.SparkClusterConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;


public class SetupStrategyOverHadoop implements ClusterSetupStrategy
{
    final TrackerOperation po;
    final SparkImpl manager;
    final SparkClusterConfig config;
    private Environment environment;
    private Set<ContainerHost> nodesToInstallSpark;


    public SetupStrategyOverHadoop( TrackerOperation po, SparkImpl manager, SparkClusterConfig config,
                                    Environment environment )
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
        if ( config.getMasterNodeId() == null )
        {
            throw new ClusterSetupException( "Master node not specified" );
        }
        if ( CollectionUtil.isCollectionEmpty( config.getSlaveIds() ) )
        {
            throw new ClusterSetupException( "No slave nodes" );
        }

        ContainerHost master;
        try
        {
            master = environment.getContainerHostById( config.getMasterNodeId() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException(
                    String.format( "Master %s not found in the environment", config.getMasterNodeId() ) );
        }

        if ( !master.isConnected() )
        {
            throw new ClusterSetupException( "Master is not connected" );
        }

        Set<ContainerHost> slaves;
        try
        {
            slaves = environment.getContainerHostsByIds( config.getSlaveIds() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( e );
        }

        if ( slaves.size() > config.getSlaveIds().size() )
        {
            throw new ClusterSetupException( "Fewer slaves found in the environment than indicated" );
        }

        for ( ContainerHost slave : slaves )
        {
            if ( !slave.isConnected() )
            {
                throw new ClusterSetupException(
                        String.format( "Container %s is not connected", slave.getHostname() ) );
            }
        }

        // check Hadoop cluster
        HadoopClusterConfig hc = manager.hadoopManager.getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }
        if ( !hc.getAllNodes().containsAll( config.getAllNodesIds() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }

        po.addLog( "Checking prerequisites..." );

        //gather all nodes
        final Set<ContainerHost> allNodes = Sets.newHashSet( master );
        allNodes.addAll( slaves );

        //check if node belongs to some existing spark cluster
        List<SparkClusterConfig> sparkClusters = manager.getClusters();
        for ( ContainerHost node : allNodes )
        {
            for ( SparkClusterConfig cluster : sparkClusters )
            {
                if ( cluster.getAllNodesIds().contains( node.getId() ) )
                {
                    throw new ClusterSetupException(
                            String.format( "Node %s already belongs to Spark cluster %s", node.getHostname(),
                                    cluster.getClusterName() ) );
                }
            }
        }

        nodesToInstallSpark = Sets.newHashSet();

        //check hadoop installation & filter nodes needing Spark installation
        RequestBuilder checkInstalledCommand = manager.getCommands().getCheckInstalledCommand();

        for ( Iterator<ContainerHost> iterator = allNodes.iterator(); iterator.hasNext(); )
        {
            final ContainerHost node = iterator.next();
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( !result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    nodesToInstallSpark.add( node );
                }
                if ( !result.getStdOut()
                            .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
                {
                    po.addLog(
                            String.format( "Node %s has no Hadoop installation. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getSlaveIds().remove( node.getId() );
                    iterator.remove();
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
            }
        }

        if ( config.getSlaveIds().isEmpty() )
        {
            throw new ClusterSetupException( "No slave nodes eligible for installation" );
        }
        if ( !allNodes.contains( master ) )
        {
            throw new ClusterSetupException( "Master node was omitted" );
        }
    }


    private void configure() throws ClusterSetupException
    {

        if ( !nodesToInstallSpark.isEmpty() )
        {
            po.addLog( "Installing Spark..." );
            //install spark
            RequestBuilder installCommand = manager.getCommands().getInstallCommand();
            for ( ContainerHost node : nodesToInstallSpark )
            {
                CommandResult result = executeCommand( node, installCommand );
                checkInstalled( node, result );
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
            throw new ClusterSetupException( e );
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


    public CommandResult executeCommand( ContainerHost host, RequestBuilder command ) throws ClusterSetupException
    {

        CommandResult result;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( e );
        }
        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
        return result;
    }

    public void checkInstalled( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( manager.getCommands().getCheckInstalledCommand() );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( SparkClusterConfig.PRODUCT_PACKAGE ) ) )
        {
            po.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
