package org.safehaus.subutai.plugin.sqoop.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.sqoop.api.DataSourceType;
import org.safehaus.subutai.plugin.sqoop.api.Sqoop;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;
import org.safehaus.subutai.plugin.sqoop.api.setting.ExportSetting;
import org.safehaus.subutai.plugin.sqoop.api.setting.ImportParameter;
import org.safehaus.subutai.plugin.sqoop.api.setting.ImportSetting;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;


public class RestServiceImpl implements RestService
{
    private static final String OPERATION_ID = "OPERATION_ID";
    private Tracker tracker;
    private Sqoop sqoopManager;


    public void setSqoopManager( Sqoop sqoopManager )
    {
        this.sqoopManager = sqoopManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Response getClusters()
    {
        List<SqoopConfig> configs = sqoopManager.getClusters();
        ArrayList<String> clusterNames = Lists.newArrayList();

        for ( SqoopConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    public Response getCluster( String clusterName )
    {
        SqoopConfig config = sqoopManager.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + "cluster not found" )
                           .build();
        }

        String cluster = JsonUtil.GSON.toJson( config );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    public Response installCluster( String clusterName, String hadoopClusterName, String nodes )
    {
        Set<UUID> uuidSet = new HashSet<>(  );
        SqoopConfig config = new SqoopConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );

        String[] arr = nodes.replaceAll( "\\s+", "" ).split( "," );
        for ( String node : arr )
        {
            uuidSet.add( UUID.fromString( node ) );
        }
        config.setNodes( uuidSet );

        UUID uuid = sqoopManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sqoopManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sqoopManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response destroyNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( sqoopManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sqoopManager.destroyNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response importData( String dataSourceType, String importAllTables, String datasourceDatabase,
                                String datasourceTableName, String datasourceColumnFamily )
    {
        ImportSetting settings = new ImportSetting();

        DataSourceType type = DataSourceType.valueOf( dataSourceType );
        settings.setType( type );

        if ( !Strings.isNullOrEmpty( importAllTables ) )
        {
            settings.addParameter( ImportParameter.IMPORT_ALL_TABLES, importAllTables );
        }

        if ( !Strings.isNullOrEmpty( datasourceDatabase ) )
        {
            settings.addParameter( ImportParameter.DATASOURCE_DATABASE, datasourceDatabase );
        }

        if ( !Strings.isNullOrEmpty( datasourceTableName ) )
        {
            settings.addParameter( ImportParameter.DATASOURCE_TABLE_NAME, datasourceTableName );
        }

        if ( !Strings.isNullOrEmpty( datasourceColumnFamily ) )
        {
            settings.addParameter( ImportParameter.DATASOURCE_COLUMN_FAMILY, datasourceColumnFamily );
        }

        UUID uuid = sqoopManager.importData( settings );

        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    public Response exportData( String hdfsPath )
    {
        ExportSetting setting = new ExportSetting();
        setting.setHdfsPath( hdfsPath );

        UUID uuid = sqoopManager.exportData( setting );

        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( SqoopConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    state = po.getState();
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 90 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( SqoopConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( po.getLog() ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }

}
