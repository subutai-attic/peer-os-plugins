package org.safehaus.subutai.plugin.elasticsearch.impl;


import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class ESSetupStrategy implements ClusterSetupStrategy
{

    private final ElasticsearchClusterConfiguration config;
    private final ElasticsearchImpl elasticsearchManager;
    private final TrackerOperation po;


    public ESSetupStrategy( final ElasticsearchClusterConfiguration elasticsearchClusterConfiguration,
                            TrackerOperation po, ElasticsearchImpl elasticsearchManager )
    {
        Preconditions.checkNotNull( elasticsearchClusterConfiguration, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( elasticsearchManager, "elasticsearchManager manager is null" );

        this.config = elasticsearchClusterConfiguration;
        this.po = po;
        this.elasticsearchManager = elasticsearchManager;
    }


    @Override
    public ElasticsearchClusterConfiguration setup() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) ||
                config.getNodes().isEmpty() || config.getEnvironmentId() == null )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( elasticsearchManager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        Environment environment =
                elasticsearchManager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );

        if ( environment == null )
        {
            throw new ClusterSetupException( "Environment not found" );
        }

        if ( environment.getContainerHostsByIds( config.getNodes() ).size() < config.getNodes().size() )
        {
            throw new ClusterSetupException( "Fewer nodes found in environment than specified" );
        }

        try
        {
            new ClusterConfiguration( elasticsearchManager, po ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException ex )
        {
            throw new ClusterSetupException( ex.getMessage() );
        }

        po.addLog( "Saving cluster information to database..." );

        elasticsearchManager.getPluginDAO()
                            .saveInfo( ElasticsearchClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster information saved to database" );

        return config;
    }
}
