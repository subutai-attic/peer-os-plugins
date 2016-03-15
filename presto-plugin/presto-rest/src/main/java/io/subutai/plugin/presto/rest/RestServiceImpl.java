package io.subutai.plugin.presto.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.ws.rs.core.Response;

import io.subutai.common.host.HostInterface;
import io.subutai.plugin.presto.rest.pojo.VersionPojo;
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
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.presto.api.Presto;
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import io.subutai.plugin.presto.rest.pojo.ContainerPojo;
import io.subutai.plugin.presto.rest.pojo.PrestoPojo;
import org.json.JSONArray;
import org.json.JSONObject;

public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Presto prestoManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;


    public RestServiceImpl( final Presto prestoManager )
    {
        this.prestoManager = prestoManager;
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
        List<PrestoClusterConfig> configs = prestoManager.getClusters();
        Set<String> clusterNames = Sets.newHashSet();

        for ( PrestoClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        PrestoClusterConfig config = prestoManager.getCluster( clusterName );

        String cluster = JsonUtil.GSON.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName, final String masterNode,
                                    final String workers )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( masterNode );
        Preconditions.checkNotNull( workers );

        PrestoClusterConfig config = new PrestoClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setCoordinatorNode( masterNode );

        List<String> hosts = JsonUtil.fromJson( workers, new TypeToken<List<String>>()
        {
        }.getType() );


        for ( String node : hosts )
        {
            config.getWorkers().add( node );
        }

        UUID uuid = prestoManager.installCluster( config );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addWorkerNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.addWorkerNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyWorkerNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.destroyWorkerNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.startNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


	@Override
	public Response startNodes (String clusterName, String lxcHosts)
	{
		Preconditions.checkNotNull( clusterName );
		Preconditions.checkNotNull( lxcHosts );
		JSONArray arr = new JSONArray (lxcHosts);
		List <String> names = new ArrayList<> ();
		for (int i = 0; i < arr.length (); ++i)
		{
			JSONObject obj = arr.getJSONObject (i);
			String name = obj.getString ("name");
			names.add (name);
		}

		if( names == null || names.isEmpty() )
		{
			return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
		}

		int errors = 0;

		for( int i = 0; i < names.size(); ++i )
		{
			UUID uuid = prestoManager.startNode ( clusterName, names.get (i));
			OperationState state = waitUntilOperationFinish( uuid );
			Response response =createResponse( uuid, state );

			if( response.getStatus() != 200 )
			{
				errors++;
			}
		}

		if( errors > 0 )
		{
			return Response.status( Response.Status.EXPECTATION_FAILED ).entity( errors + " nodes are failed to execute" ).build();
		}

		return Response.ok().build();
	}



    @Override
    public Response stopNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.stopNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


	@Override
	public Response stopNodes (String clusterName, String lxcHosts)
	{
		Preconditions.checkNotNull( clusterName );
		Preconditions.checkNotNull( lxcHosts );
		JSONArray arr = new JSONArray (lxcHosts);
		List <String> names = new ArrayList<>();
		for (int i = 0; i < arr.length (); ++i)
		{
			JSONObject obj = arr.getJSONObject (i);
			String name = obj.getString ("name");
			names.add (name);
		}

		if( names == null || names.isEmpty() )
		{
			return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
		}

		int errors = 0;

		for( int i = 0; i < names.size(); ++i )
		{
			UUID uuid = prestoManager.stopNode ( clusterName, names.get (i) );
			OperationState state = waitUntilOperationFinish( uuid );
			Response response =createResponse( uuid, state );

			if( response.getStatus() != 200 )
			{
				errors++;
			}
		}

		if( errors > 0 )
		{
			return Response.status( Response.Status.EXPECTATION_FAILED ).entity( errors + " nodes are failed to execute" ).build();
		}

		return Response.ok().build();
	}




    @Override
    public Response checkNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.checkNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.startAllNodes( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.stopAllNodes( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( prestoManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = prestoManager.checkAllNodes( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private PrestoPojo updateConfig( PrestoClusterConfig config )
    {
        PrestoPojo pojo = new PrestoPojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getWorkers() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
				HostInterface hostInterface = ch.getInterfaceByName ("eth0");
                UUID uuidStatus = prestoManager.checkNode( config.getClusterName(), ch.getHostname() );
                containerPojoSet.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp (),
                        checkStatus( tracker, uuidStatus ) ) );
            }

            pojo.setWorkers( containerPojoSet );

            ContainerHost ch = environment.getContainerHostById( config.getCoordinatorNode() );
			HostInterface hostInterface = ch.getInterfaceByName ("eth0");
            UUID uuid = prestoManager.checkNode( config.getClusterName(), ch.getHostname() );
            pojo.setCoordinator(
                    new ContainerPojo( ch.getHostname(), config.getCoordinatorNode(), hostInterface.getIp (),
                            checkStatus( tracker, uuid ) ) );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        PrestoClusterConfig oozieClusterConfig = prestoManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( oozieClusterConfig.getHadoopClusterName() );

        Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
        nodes.removeAll( oozieClusterConfig.getAllNodes() );
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
//            return Response.status( Response.Status.NOT_FOUND ).build();
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private String checkStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( PrestoClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Running as" ) )
                    {
                        state = "RUNNING";
                    }
                    else if ( po.getLog().contains( "Not running" ) )
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
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
        PrestoClusterConfig config = prestoManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            prestoManager.saveConfig( config );
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


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( PrestoClusterConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( PrestoClusterConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 200 * 1000 ) )
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
