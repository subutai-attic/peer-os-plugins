package io.subutai.plugin.hipi.impl;


import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hipi.api.HipiConfig;


public class HipiSetupStrategy implements ClusterSetupStrategy
{
    final HipiImpl manager;
    final HipiConfig config;
    final TrackerOperation trackerOperation;
    CommandUtil commandUtil = new CommandUtil();
    private Environment environment;
    private Set<EnvironmentContainerHost> nodes;


    public HipiSetupStrategy( HipiImpl manager, HipiConfig config, TrackerOperation trackerOperation )
    {
        this.manager = manager;
        this.config = config;
        this.trackerOperation = trackerOperation;
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
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            throw new ClusterSetupException( "Invalid cluster name" );
        }
        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            throw new ClusterSetupException( "Invalid Hadoop cluster name" );
        }

        if ( CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Nodes are not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( config.getHadoopClusterName() );

        if ( hadoopClusterConfig == null )
        {
            throw new ClusterSetupException(
                    String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
        }

        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Hadoop environment not found: %s", e ) );
        }


        if ( !hadoopClusterConfig.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    String.format( "Not all nodes belong to Hadoop cluster %s", config.getHadoopClusterName() ) );
        }

        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed to obtain hadoop environment containers: %s", e ) );
        }

        if ( nodes.size() < config.getNodes().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Found only %d nodes in environment whereas %d expected", nodes.size(),
                            config.getNodes().size() ) );
        }

        List<HipiConfig> hipiClusters = manager.getClusters();

        for ( HipiConfig cluster : hipiClusters )
        {
            for ( EnvironmentContainerHost node : nodes )
            {
                if ( cluster.getNodes().contains( node.getId() ) )
                {
                    throw new ClusterSetupException(
                            String.format( "Node %s already belongs to cluster %s", node.getHostname(),
                                    cluster.getClusterName() ) );
                }
            }
        }

        for ( EnvironmentContainerHost node : nodes )
        {
            if ( !node.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }

            try
            {
                RequestBuilder statusCommand =
                        new RequestBuilder( CommandFactory.build( NodeOperationType.CHECK_INSTALLATION ) );
                CommandResult result = commandUtil.execute( statusCommand, node );
                if ( result.getStdOut().contains( HipiConfig.PRODUCT_PACKAGE ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Hipi installed. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
                else if ( !result.getStdOut()
                                 .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s has no Hadoop installation. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
            }
        }
        if ( config.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation" );
        }
    }


    private void configure() throws ClusterSetupException
    {

        trackerOperation.addLog( "Updating Hipi..." );
        config.setEnvironmentId( environment.getId() );


        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Hipi..." );


        for ( EnvironmentContainerHost node : nodes )
        {
            try
            {
                RequestBuilder installCommand =
                        new RequestBuilder( CommandFactory.build( NodeOperationType.INSTALL ) ).withTimeout( 300 );
                CommandResult result = commandUtil.execute( installCommand, node );
                checkInstalled( node, result );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Error while installing Hipi on container %s; %s", node.getHostname(),
                                e.getMessage() ) );
            }
        }
        try
        {
            manager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( e );
        }

        trackerOperation.addLog( "Installation info successfully saved" );
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = commandUtil
                    .execute( new RequestBuilder( CommandFactory.build( NodeOperationType.CHECK_INSTALLATION ) ),
                            host );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( HipiConfig.PRODUCT_PACKAGE ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
