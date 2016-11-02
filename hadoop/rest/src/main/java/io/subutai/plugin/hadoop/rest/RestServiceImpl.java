package io.subutai.plugin.hadoop.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.CollectionUtil;
import io.subutai.common.util.JsonUtil;
import io.subutai.common.util.StringUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.rest.pojo.ContainerPojo;
import io.subutai.plugin.hadoop.rest.pojo.HadoopPojo;
import io.subutai.plugin.hadoop.rest.pojo.VersionPojo;


public class RestServiceImpl implements RestService
{

    private Hadoop hadoopManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Hadoop hadoopManager, final Tracker tracker,
                            final EnvironmentManager environmentManager )
    {
        this.hadoopManager = hadoopManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    @Override
    public Response listClusters()
    {
        List<HadoopClusterConfig> hadoopClusterConfigList = hadoopManager.getClusters();
        ArrayList<String> clusterNames = new ArrayList<>();

        for ( HadoopClusterConfig hadoopClusterConfig : hadoopClusterConfigList )
        {
            clusterNames.add( hadoopClusterConfig.getClusterName() );
        }


        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );

        String cluster = JsonUtil.GSON.toJson( updateStatus( config ) );

        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response configureCluster( final String config )
    {
        TrimmedHadoopConfig trimmedHadoopConfig = JsonUtil.fromJson( config, TrimmedHadoopConfig.class );
        Set<String> excludedSlaves = new HashSet<>();
        HadoopClusterConfig hadoopConfig = new HadoopClusterConfig();
        hadoopConfig.setClusterName( validateInput( trimmedHadoopConfig.getClusterName(), true ) );
        hadoopConfig.setDomainName( trimmedHadoopConfig.getDomainName() );
        hadoopConfig.setEnvironmentId( trimmedHadoopConfig.getEnvironmentId() );
        hadoopConfig.setNameNode( trimmedHadoopConfig.getNameNode() );
        hadoopConfig.setReplicationFactor( trimmedHadoopConfig.getReplicationFactor() );
        hadoopConfig.setExcludedSlaves( excludedSlaves );

        if ( !CollectionUtil.isCollectionEmpty( trimmedHadoopConfig.getSlaves() ) )
        {
            Set<String> slaveNodes = new HashSet<>();
            for ( String node : trimmedHadoopConfig.getSlaves() )
            {
                slaveNodes.add( node );
            }
            hadoopConfig.setSlaves( slaveNodes );
        }

        UUID uuid = hadoopManager.installCluster( hadoopConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( String clusterName )
    {
        UUID uuid = hadoopManager.uninstallCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNameNode( String clusterName, String hostName )
    {
        UUID uuid = hadoopManager.startNameNode( clusterName, hostName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNameNode( String clusterName, String hostName )
    {
        UUID uuid = hadoopManager.stopNameNode( clusterName, hostName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( String clusterName )
    {
        UUID uuid = hadoopManager.addNode( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String node )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( node );

        if ( hadoopManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );

        UUID uuid = hadoopManager.destroyNode( config, node );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            hadoopManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "Auto scale cannot set successfully" ).build();
        }
        if ( !scale )
        {
            message = "disabled";
        }

        return Response.status( Response.Status.OK ).entity( "Auto scale is " + message + " successfully" ).build();
    }


    @Override
    public Response getPluginInfo()
    {
        Properties prop = new Properties();
        VersionPojo pojo = new VersionPojo();
        InputStream input = null;
        try
        {
            input = getClass().getResourceAsStream( "/git.properties" );

            prop.load( input );
            pojo.setGitCommitId( prop.getProperty( "git.commit.id" ) );
            pojo.setGitCommitTime( prop.getProperty( "git.commit.time" ) );
            pojo.setGitBranch( prop.getProperty( "git.branch" ) );
            pojo.setGitCommitUserName( prop.getProperty( "git.commit.user.name" ) );
            pojo.setGitCommitUserEmail( prop.getProperty( "git.commit.user.email" ) );
            pojo.setProjectVersion( prop.getProperty( "git.build.version" ) );

            pojo.setGitBuildUserName( prop.getProperty( "git.build.user.name" ) );
            pojo.setGitBuildUserEmail( prop.getProperty( "git.build.user.email" ) );
            pojo.setGitBuildHost( prop.getProperty( "git.build.host" ) );
            pojo.setGitBuildTime( prop.getProperty( "git.build.time" ) );

            pojo.setGitClosestTagName( prop.getProperty( "git.closest.tag.name" ) );
            pojo.setGitCommitIdDescribeShort( prop.getProperty( "git.commit.id.describe-short" ) );
            pojo.setGitClosestTagCommitCount( prop.getProperty( "git.closest.tag.commit.count" ) );
            pojo.setGitCommitIdDescribe( prop.getProperty( "git.commit.id.describe" ) );
        }
        catch ( IOException ex )
        {
            ex.printStackTrace();
        }
        finally
        {
            if ( input != null )
            {
                try
                {
                    input.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }

        String projectInfo = JsonUtil.GSON.toJson( pojo );

        return Response.status( Response.Status.OK ).entity( projectInfo ).build();
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( JsonUtil.toJson( po.getLog() ) )
                           .build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( JsonUtil.toJson( JsonUtil.toJson( po.getLog() ) ) )
                           .build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
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
            if ( System.currentTimeMillis() - start > ( 60 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    private HadoopPojo updateStatus( HadoopClusterConfig config )
    {
        HadoopPojo pojo = new HadoopPojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );


            for ( final String uuid : config.getSlaves() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                UUID dataUUID = hadoopManager.statusDataNode( config.getClusterName(), ch.getHostname() );
                containerPojoSet.add( new ContainerPojo( uuid, ch.getHostname(),
                        ch.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp(),
                        checkDataStatus( tracker, dataUUID ) ) );
            }

            pojo.setSlaves( containerPojoSet );

            ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
            UUID nameNodeUUID = hadoopManager.statusNameNode( config.getClusterName(), namenode.getHostname() );
            ContainerPojo namenodePojo = new ContainerPojo( config.getNameNode(), namenode.getHostname(),
                    namenode.getInterfaceByName( Common.DEFAULT_CONTAINER_INTERFACE ).getIp(),
                    checkNamenodeStatus( tracker, nameNodeUUID ) );
            pojo.setNameNode( namenodePojo );


            String envDataSource = environment.toString().contains( "ProxyEnvironment" ) ? "hub" : "subutai";
            pojo.setEnvironmentDataSource( envDataSource );
            pojo.setClusterName( config.getClusterName() );
            pojo.setAutoScaling( config.isAutoScaling() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
        }
        catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private String checkNamenodeStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "NameNode" ) )
                    {
                        state = "RUNNING";
                    }
                    else
                    {
                        state = "STOPPED";
                    }
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
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        return state;
    }


    private String checkDataStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "DataNode" ) )
                    {
                        state = "RUNNING";
                    }
                    else
                    {
                        state = "STOPPED";
                    }
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
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        return state;
    }


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( hadoopManager.getWebModule().getAngularDependecyList() ).build();
    }
}
