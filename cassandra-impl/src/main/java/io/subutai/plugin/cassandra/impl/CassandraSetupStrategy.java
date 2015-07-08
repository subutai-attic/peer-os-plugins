package io.subutai.plugin.cassandra.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class CassandraSetupStrategy implements ClusterSetupStrategy
{

    private Environment environment;
    private CassandraClusterConfig config;
    private CassandraImpl cassandraManager;
    private TrackerOperation trackerOperation;


    public CassandraSetupStrategy( final Environment environment, final CassandraClusterConfig config,
                                   final TrackerOperation po, final CassandraImpl cassandra )
    {

        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( cassandra, "Mongo manager is null" );
        this.environment = environment;
        this.config = config;
        this.trackerOperation = po;
        this.cassandraManager = cassandra;
    }


    @Override
    public CassandraClusterConfig setup() throws ClusterSetupException
    {

        if ( Strings.isNullOrEmpty( config.getClusterName() ) ||
                Strings.isNullOrEmpty( config.getCommitLogDirectory() ) ||
                Strings.isNullOrEmpty( config.getDataDirectory() ) ||
                Strings.isNullOrEmpty( config.getSavedCachesDirectory() ) ||
                Strings.isNullOrEmpty( config.getDomainName() ) ||
                Strings.isNullOrEmpty( config.getProductName() ) ||
                Strings.isNullOrEmpty( config.getTEMPLATE_NAME() ) )
        {
            throw new ClusterSetupException( "Malformed cluster configuration" );
        }

        if ( cassandraManager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        Set<UUID> cassNodes = new HashSet<>();
        for ( ContainerHost environmentContainer : environment.getContainerHosts() )
        {
            cassNodes.add( environmentContainer.getId() );
        }
        config.setNodes( cassNodes );

        Iterator iterator = cassNodes.iterator();
        Set<UUID> seedNodes = new HashSet<>();
        while ( iterator.hasNext() )
        {
            seedNodes.add( ( UUID ) iterator.next() );
            if ( seedNodes.size() == config.getNumberOfSeeds() )
            {
                break;
            }
        }
        config.setSeedNodes( seedNodes );


        try
        {
            new ClusterConfiguration( trackerOperation, cassandraManager ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }
        return config;
    }
}
