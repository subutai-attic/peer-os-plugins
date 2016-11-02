package io.subutai.plugin.hbase.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.FormParam;
import javax.ws.rs.core.Response;

import io.subutai.common.host.HostInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.CollectionUtil;
import io.subutai.common.util.JsonUtil;
import io.subutai.common.util.StringUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.rest.pojo.ContainerPojo;
import io.subutai.plugin.hbase.rest.pojo.HbasePojo;
import io.subutai.plugin.hbase.rest.pojo.VersionPojo;


public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private HBase hbaseManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    @Override
    public Response listClusters()
    {
        List<HBaseConfig> configs = hbaseManager.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( HBaseConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String source )
    {
        HBaseConfig cluster = hbaseManager.getCluster( source );
        String clusterName = JsonUtil.toJson( updateConfig( cluster ) );
        return Response.status( Response.Status.OK ).entity( clusterName ).build();
    }


    @Override
    public Response configureCluster( final String config )
    {
        TrimmedHBaseConfig trimmedHBaseConfig = JsonUtil.fromJson( config, TrimmedHBaseConfig.class );
        HBaseConfig hbaseConfig = new HBaseConfig();
        hbaseConfig.setClusterName( validateInput( trimmedHBaseConfig.getClusterName(), true ) );
        hbaseConfig.setHadoopClusterName( trimmedHBaseConfig.getHadoopClusterName() );
        hbaseConfig.setDomainName( trimmedHBaseConfig.getDomainName() );
        hbaseConfig.setEnvironmentId( trimmedHBaseConfig.getEnvironmentId() );
        hbaseConfig.setHbaseMaster( trimmedHBaseConfig.getHmaster() );

        if ( !CollectionUtil.isCollectionEmpty( trimmedHBaseConfig.getRegionServers() ) )
        {
            Set<String> slaveNodes = new HashSet<>();
            for ( String node : trimmedHBaseConfig.getRegionServers() )
            {
                slaveNodes.add( node );
            }
            hbaseConfig.getRegionServers().addAll( slaveNodes );
        }

        UUID uuid = hbaseManager.installCluster( hbaseConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        UUID uuid = hbaseManager.uninstallCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( master );
        if ( hbaseManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hbaseManager.startNode( clusterName, lxcHostName, master );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( master );
        if ( hbaseManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hbaseManager.stopNode( clusterName, lxcHostName, master );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNodes( final String clusterName, final String lxcHosts )
    {
        return nodeOperation( clusterName, lxcHosts, true );
    }


    @Override
    public Response stopNodes( final String clusterName, final String lxcHosts )
    {
        return nodeOperation( clusterName, lxcHosts, false );
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostName )
    {
        UUID uuid = hbaseManager.addNode( clusterName, lxcHostName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String containerId )
    {
        UUID uuid = hbaseManager.destroyNode( clusterName, containerId );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String hostname )
    {
        UUID uuid = hbaseManager.checkNode( clusterName, hostname );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
        HBaseConfig config = hbaseManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            hbaseManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
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


    private HbasePojo updateConfig( HBaseConfig config )
    {
        HbasePojo pojo = new HbasePojo();
        Set<ContainerPojo> regionServers = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getRegionServers() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                UUID uuidStatus = hbaseManager.checkNode( config.getClusterName(), ch.getHostname() );
                regionServers.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp(),
                        checkRegionServerStatus( tracker, uuidStatus ) ) );
            }
            pojo.setRegionServers( regionServers );

            ContainerHost containerHost = environment.getContainerHostById( config.getHbaseMaster() );
            HostInterface hostInterface = containerHost.getInterfaceByName( "eth0" );
            UUID uuidStatus = hbaseManager.checkNode( config.getClusterName(), containerHost.getHostname() );
            pojo.setHbaseMaster(
                    new ContainerPojo( containerHost.getHostname(), config.getHbaseMaster(), hostInterface.getIp(),
                            checkHbaseMasterStatus( tracker, uuidStatus ) ) );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private String checkRegionServerStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "HRegionServer" ) )
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


    private String checkHbaseMasterStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "HMaster" ) && po.getLog().contains( "HQuorumPeer" ) )
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


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        HBaseConfig hBaseConfig = hbaseManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( hBaseConfig.getHadoopClusterName() );

        try
        {
            Environment environment = environmentManager.loadEnvironment( hadoopConfig.getEnvironmentId() );

            Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
            nodes.removeAll( hBaseConfig.getAllNodes() );

            if ( !nodes.isEmpty() )
            {
                Set<EnvironmentContainerHost> hosts;
                try
                {
                    hosts = environmentManager.loadEnvironment( hadoopConfig.getEnvironmentId() )
                                              .getContainerHostsByIds( nodes );

                    for ( final EnvironmentContainerHost host : hosts )
                    {
                        hostsName.add( host.getHostname() );
                    }
                }
                catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                LOG.info( "All nodes in corresponding Hadoop cluster have Nutch installed" );
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment: ", e );
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_NAME, uuid );
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


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 90 * 100000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response nodeOperation( String clusterName, String lxcHosts, boolean startNode )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );
        List<String> hosts;


        if ( hbaseManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        try
        {
            hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>()
            {
            }.getType() );
        }
        catch ( Exception e )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( JsonUtil.toJson( "Bad input form" + e ) )
                           .build();
        }

        int errors = 0;

        for ( String host : hosts )
        {
            UUID uuid;
            if ( startNode )
            {
                uuid = hbaseManager.startNode( clusterName, host, false );
            }
            else
            {
                uuid = hbaseManager.stopNode( clusterName, host, false );
            }

            OperationState state = waitUntilOperationFinish( uuid );

            Response response = createResponse( uuid, state );

            if ( response.getStatus() != 200 )
            {
                errors++;
            }
        }

        if ( errors > 0 )
        {
            return Response.status( Response.Status.EXPECTATION_FAILED )
                           .entity( errors + " nodes are failed to execute" ).build();
        }

        return Response.ok().build();
    }


    public HBase getHbaseManager()
    {
        return hbaseManager;
    }


    public void setHbaseManager( final HBase hbaseManager )
    {
        this.hbaseManager = hbaseManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( hbaseManager.getWebModule().getAngularDependecyList() ).build();
    }


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }
}
