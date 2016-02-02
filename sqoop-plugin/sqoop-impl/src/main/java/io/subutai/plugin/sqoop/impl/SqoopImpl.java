package io.subutai.plugin.sqoop.impl;


import java.util.List;
import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.sqoop.api.SqoopConfig;
import io.subutai.plugin.sqoop.api.setting.ExportSetting;
import io.subutai.plugin.sqoop.api.setting.ImportSetting;
import io.subutai.plugin.sqoop.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.sqoop.impl.handler.NodeOperationHandler;


public class SqoopImpl extends SqoopBase
{

    public SqoopImpl( PluginDAO pluginDAO )
    {
        super( pluginDAO );
    }


    @Override
    public UUID installCluster( SqoopConfig config )
    {
        AbstractOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID installCluster( SqoopConfig config, HadoopClusterConfig hadoopConfig )
    {
        ClusterOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.INSTALL );
        h.setHadoopConfig( hadoopConfig );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        SqoopConfig config = getCluster( clusterName );
        AbstractOperationHandler h = new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public List<SqoopConfig> getClusters()
    {
        return pluginDAO.getInfo( SqoopConfig.PRODUCT_KEY, SqoopConfig.class );
    }


    @Override
    public SqoopConfig getCluster( String clusterName )
    {
        return pluginDAO.getInfo( SqoopConfig.PRODUCT_KEY, clusterName, SqoopConfig.class );
    }


    @Override
    public UUID isInstalled( String clusterName, String hostname )
    {
        SqoopConfig config = getCluster( clusterName );
        AbstractOperationHandler h = new NodeOperationHandler( this, config, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID destroyNode( String clusterName, String hostname )
    {
        SqoopConfig config = getCluster( clusterName );
        AbstractOperationHandler h = new NodeOperationHandler( this, config, hostname, NodeOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID addNode( final String clusterName, final String lxcHostname )
    {
        // N/A for Sqoop installation
        return null;
    }


    @Override
    public UUID exportData( ExportSetting settings )
    {
        SqoopConfig config = getCluster( settings.getClusterName() );
        NodeOperationHandler h =
                new NodeOperationHandler( this, config, settings.getHostname(), NodeOperationType.EXPORT );
        h.setExportSettings( settings );

        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID importData( ImportSetting settings )
    {
        SqoopConfig config = getCluster( settings.getClusterName() );
        NodeOperationHandler h =
                new NodeOperationHandler( this, config, settings.getHostname(), NodeOperationType.IMPORT );
        h.setImportSettings( settings );

        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public String fetchDatabases( ImportSetting importSetting )
    {
        String databases = null;
        SqoopConfig config = getCluster( importSetting.getClusterName() );
        String query = CommandFactory.fetchDatabasesQuery( importSetting );
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            try
            {
                ContainerHost containerHost = environment.getContainerHostByHostname( importSetting.getHostname() );
                try
                {
                    CommandResult result = containerHost.execute( new RequestBuilder( query ) );
                    if ( result.hasSucceeded() )
                    {
                        databases = result.getStdOut();
                    }
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return databases;
    }


    @Override
    public String fetchTables( ImportSetting importSetting )
    {
        String tables = null;
        SqoopConfig config = getCluster( importSetting.getClusterName() );
        String query = CommandFactory.fetchTablesQuery( importSetting );
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            try
            {
                ContainerHost containerHost = environment.getContainerHostByHostname( importSetting.getHostname() );
                try
                {
                    CommandResult result = containerHost.execute( new RequestBuilder( query ) );
                    if ( result.hasSucceeded() )
                    {
                        tables = result.getStdOut();
                    }
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return tables;
    }


    @Override
    public String reviewExportQuery( ExportSetting settings )
    {
        return CommandFactory.build( NodeOperationType.EXPORT, settings );
    }


    @Override
    public String reviewImportQuery( ImportSetting settings )
    {
        return CommandFactory.build( NodeOperationType.IMPORT, settings );
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, SqoopConfig config, TrackerOperation to )
    {
        return new SetupStrategyOverHadoop( this, config, env, to );
    }
}

