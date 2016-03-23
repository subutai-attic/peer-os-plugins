package io.subutai.plugin.mongodb.impl;


import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;


/**
* This is a mongodb cluster setup strategy.
*/
public class MongoDbSetupStrategy implements ClusterSetupStrategy
{

    private MongoImpl mongoManager;
    private TrackerOperation po;
    private MongoClusterConfig config;
    private Environment environment;


    public MongoDbSetupStrategy( Environment environment, MongoClusterConfig config, TrackerOperation po,
                                 MongoImpl mongoManager )
    {

        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( mongoManager, "Mongo manager is null" );

        this.environment = environment;
        this.mongoManager = mongoManager;
        this.po = po;
        this.config = config;
    }


    @Override
    public MongoClusterConfig setup() throws ClusterSetupException
    {

        if ( Strings.isNullOrEmpty( config.getClusterName() ) ||
                Strings.isNullOrEmpty( config.getDomainName() ) ||
                Strings.isNullOrEmpty( config.getReplicaSetName() ) ||
                Strings.isNullOrEmpty( MongoClusterConfig.getTemplateName() ) ||
                !Sets.newHashSet( 1, 2, 3 ).contains( config.getNumberOfConfigServers() ) ||
                !Range.closed( 1, 3 ).contains( config.getNumberOfRouters() ) ||
                !Sets.newHashSet( 1, 2, 3, 4, 5, 6, 7 ).contains( config.getNumberOfDataNodes() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getCfgSrvPort() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getRouterPort() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getDataNodePort() ) )
        {
            throw new ClusterSetupException( "Malformed cluster configuration" );
        }

        if ( mongoManager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        if ( environment.getContainerHosts().isEmpty() )
        {
            throw new ClusterSetupException( "Environment has no nodes" );
        }

        try
        {
            new ClusterConfiguration( po, mongoManager ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }
        return config;
    }
}
