package org.safehaus.subutai.plugin.sqoop.impl;


import java.util.List;
import java.util.UUID;


import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;
import org.safehaus.subutai.plugin.sqoop.api.setting.ExportSetting;
import org.safehaus.subutai.plugin.sqoop.api.setting.ImportSetting;
import org.safehaus.subutai.plugin.sqoop.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.sqoop.impl.handler.NodeOperationHandler;


public class SqoopImpl extends SqoopBase
{

    public SqoopImpl()
    {
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
        NodeOperationHandler h = new NodeOperationHandler( this, config, settings.getHostname(),
                                                           NodeOperationType.EXPORT );
        h.setExportSettings( settings );

        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID importData( ImportSetting settings )
    {
        SqoopConfig config = getCluster( settings.getClusterName() );
        NodeOperationHandler h = new NodeOperationHandler( this, config, settings.getHostname(),
                                                           NodeOperationType.IMPORT );
        h.setImportSettings( settings );

        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public String reviewExportQuery( ExportSetting settings ){
        return CommandFactory.build( NodeOperationType.EXPORT, settings );
    }

    @Override
    public String reviewImportQuery( ImportSetting settings ){
        return CommandFactory.build( NodeOperationType.IMPORT, settings );
    }

    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, SqoopConfig config, TrackerOperation to )
    {
        return new SetupStrategyOverHadoop( this, config, env, to );
    }
}

