package io.subutai.plugin.zookeeper.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.ws.rs.core.Response;

import io.subutai.plugin.zookeeper.rest.pojo.VersionPojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.rest.pojo.ContainerPojo;
import io.subutai.plugin.zookeeper.rest.pojo.ZookeeperPojo;


/**
 * REST implementation of Zookeeper API
 */

public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Zookeeper zookeeperManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;


    public RestServiceImpl( final Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }


    public void setZookeeperManager( Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }



	@Override
	public Response getPluginInfo()
	{
		Properties prop = new Properties();
		VersionPojo pojo = new VersionPojo();
		InputStream input = null;
		try
		{
			input = getClass().getResourceAsStream("/git.properties");

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



    @Override
    public Response listClusters()
    {
        List<ZookeeperClusterConfig> configs = zookeeperManager.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( ZookeeperClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        return Response.status( Response.Status.OK ).entity( JsonUtil.toJson( clusterNames ) ).build();
    }


    @Override
    public Response getCluster( final String source )
    {
        String clusters = JsonUtil.toJson( updateConfig( zookeeperManager.getCluster( source ) ) );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response configureCluster( final String environmentId, final String clusterName, final String nodes )
    {
        Preconditions.checkNotNull( environmentId );
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodes );

        Environment environment = null;
        try
        {
            environment = environmentManager.loadEnvironment( environmentId );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        if ( environment == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "Could not find environment with id : " + environmentId ).build();
        }
        if ( zookeeperManager.getCluster( clusterName ) != null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "There is already a cluster with same name!" ).build();
        }

        ZookeeperClusterConfig config = new ZookeeperClusterConfig();
        config.setEnvironmentId( environmentId );
        config.setClusterName( clusterName );
        config.setSetupType( SetupType.OVER_ENVIRONMENT );

        List<String> hosts = JsonUtil.fromJson( nodes, new TypeToken<List<String>>()
        {
        }.getType() );

        for ( String node : hosts )
        {
            config.getNodes().add( node );
        }

        UUID uuid = zookeeperManager.configureEnvironmentCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName,
                                    final String environmentId, final String nodes )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( nodes );

        ZookeeperClusterConfig config = new ZookeeperClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( environmentId );
        config.setSetupType( SetupType.OVER_HADOOP );

        List<String> hosts = JsonUtil.fromJson( nodes, new TypeToken<List<String>>()
        {
        }.getType() );

        for ( String node : hosts )
        {
            config.getNodes().add( node );
        }

        UUID uuid = zookeeperManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyCluster( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.startNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startAllNodes( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.startAllNodes( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkAllNodes( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.checkAllNodes( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.stopNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopAllNodes( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.stopAllNodes( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNodes( String clusterName, String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );

        List<String> hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>()
        {
        }.getType() );

        if ( hosts == null || hosts.isEmpty() )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
        }

        int errors = 0;

        for ( String host : hosts )
        {
            UUID uuid = zookeeperManager.startNode( clusterName, host );
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


    @Override
    public Response stopNodes( String clusterName, String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );


        List<String> hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>()
        {
        }.getType() );

        if ( hosts == null || hosts.isEmpty() )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
        }

        int errors = 0;

        for ( String host : hosts )
        {
            UUID uuid = zookeeperManager.stopNode( clusterName, host );
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


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.destroyNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.checkNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.addNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNodeStandalone( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( zookeeperManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = zookeeperManager.addNode( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
        ZookeeperClusterConfig config = zookeeperManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            zookeeperManager.saveConfig( config );
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
    public Response getAvailableNodes( final String clusterName )
    {
        String hostsJson = null;
        Set<String> hostsName = Sets.newHashSet();
        ZookeeperClusterConfig zookeeperClusterConfig = zookeeperManager.getCluster( clusterName );

        switch ( zookeeperClusterConfig.getSetupType() )
        {
            case OVER_HADOOP:
                HadoopClusterConfig hadoopConfig =
                        hadoopManager.getCluster( zookeeperClusterConfig.getHadoopClusterName() );
                Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
                nodes.removeAll( zookeeperClusterConfig.getNodes() );
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
                    LOG.info( "All nodes in corresponding Hadoop cluster have Zookeeper installed" );
                }

                hostsJson = JsonUtil.GSON.toJson( hostsName );
                return Response.status( Response.Status.OK ).entity( hostsJson ).build();
            case OVER_ENVIRONMENT:

                Environment environment = null;
                try
                {
                    environment = environmentManager.loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    LOG.error( String.format( "Couldn't get environment with id: %s",
                            zookeeperClusterConfig.getEnvironmentId() ), e );
                }
                Set<EnvironmentContainerHost> environmentHosts = environment.getContainerHosts();
                Set<EnvironmentContainerHost> zookeeperHosts = new HashSet<>();
                try
                {
                    zookeeperHosts.addAll( environment.getContainerHostsByIds( zookeeperClusterConfig.getNodes() ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( String.format( "Couldn't get some container hosts with ids: %s",
                            zookeeperClusterConfig.getNodes().toString() ), e );
                }
                environmentHosts.removeAll( zookeeperHosts );

                for ( final EnvironmentContainerHost environmentHost : environmentHosts )
                {
                    hostsName.add( environmentHost.getHostname() );
                }

                hostsJson = JsonUtil.GSON.toJson( hostsName );
                return Response.status( Response.Status.OK ).entity( hostsJson ).build();
        }

        return null;
    }


    private ZookeeperPojo updateConfig( ZookeeperClusterConfig config )
    {
        ZookeeperPojo pojo = new ZookeeperPojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getNodes() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                UUID uuidStatus = zookeeperManager.checkNode( config.getClusterName(), ch.getHostname() );
                containerPojoSet.add( new ContainerPojo( ch.getHostname(), uuid, ch.getIpByInterfaceName( "eth0" ),
                        checkStatus( tracker, uuidStatus ) ) );
            }

            pojo.setNodes( containerPojoSet );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private String checkStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Zookeeper Server is running" ) )
                    {
                        state = "RUNNING";
                    }
                    else if ( po.getLog().contains( "Zookeeper Server is not running" ) )
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


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY, uuid );
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
            TrackerOperationView po = tracker.getTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 900 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
