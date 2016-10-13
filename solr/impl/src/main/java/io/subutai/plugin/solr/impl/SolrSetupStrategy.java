package io.subutai.plugin.solr.impl;


import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.solr.api.SolrClusterConfig;


/**
 * Solr cluster setup strategy
 */
public class SolrSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SolrSetupStrategy.class );
    private SolrImpl manager;
    private TrackerOperation po;
    private SolrClusterConfig config;


    public SolrSetupStrategy( final SolrImpl manager, final TrackerOperation po, final SolrClusterConfig config,
                              final Environment environment )
    {
        Preconditions.checkNotNull( manager, "Solr Manager is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );
        Preconditions.checkNotNull( config, "Solr config is null" );
        Preconditions.checkNotNull( environment, "Environment is null" );
        this.manager = manager;
        this.po = po;
        this.config = config;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment", e );
            po.addLogFailed( "Error getting environment" );
            throw new ClusterSetupException( e );
        }
        if ( Strings.isNullOrEmpty( config.getClusterName() ) || config.getNumberOfNodes() <= 0 )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        if ( environment.getContainerHosts().isEmpty() )
        {
            throw new ClusterSetupException( "Environment has no nodes" );
        }

        if ( environment.getContainerHosts().size() < config.getNumberOfNodes() )
        {
            throw new ClusterSetupException( String.format( "Environment has %d nodes but %d nodes are required",
                    environment.getContainerHosts().size(), config.getNumberOfNodes() ) );
        }

        Set<ContainerHost> clusterHosts = new HashSet<>();
        Set<String> solrNodes = new HashSet<>( config.getNodes() );
        for ( ContainerHost host : environment.getContainerHosts() )
        {
            if ( host.getTemplateName().equals( SolrClusterConfig.TEMPLATE_NAME ) && solrNodes
                    .contains( host.getId() ) )
            {
                clusterHosts.add( host );
            }
        }

        try
        {
            new ClusterConfiguration( manager, po ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            e.printStackTrace();
        }

        po.addLog( "Saving cluster information to database..." );

        manager.getPluginDAO().saveInfo( SolrClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster information saved to database" );

        return config;
    }
}
