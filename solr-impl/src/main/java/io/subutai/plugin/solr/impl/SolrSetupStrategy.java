package io.subutai.plugin.solr.impl;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


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
        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
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
        Set<UUID> solrNodes = new HashSet<>( config.getNodes() );
        for ( ContainerHost host : environment.getContainerHosts() )
        {
            if ( host.getTemplateName().equals( SolrClusterConfig.TEMPLATE_NAME ) && solrNodes
                    .contains( host.getId() ) )
            {
                clusterHosts.add( host );
            }
        }

        po.addLog( "Starting solr service on nodes" );


        for ( final ContainerHost clusterHost : clusterHosts )
        {
            try
            {
                clusterHost.execute( new RequestBuilder( Commands.startCommand ).withTimeout( 30 ) );
            }
            catch ( CommandException e )
            {
                String msg = String.format( "Error executing command %s on node %s", Commands.startCommand,
                        clusterHost.getHostname() );
                LOGGER.error( msg, e );
                po.addLogFailed( msg );
            }
        }

        po.addLog( "Saving cluster information to database..." );

        manager.getPluginDAO().saveInfo( SolrClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster information saved to database" );

        return config;
    }
}
