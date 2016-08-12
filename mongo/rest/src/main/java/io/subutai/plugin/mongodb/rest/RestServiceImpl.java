package io.subutai.plugin.mongodb.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.CollectionUtil;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mongodb.api.Mongo;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.NodeType;
import io.subutai.plugin.mongodb.rest.pojo.ContainerPojo;
import io.subutai.plugin.mongodb.rest.pojo.MongoPojo;
import io.subutai.plugin.mongodb.rest.pojo.VersionPojo;


/**
 * REST implementation of MongoDB API
 */

public class RestServiceImpl implements RestService
{

    private Mongo mongo;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Mongo mongo )
    {
        this.mongo = mongo;
    }


    @Override
    public Response listClusters()
    {
        List<MongoClusterConfig> configs = mongo.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( MongoClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        MongoClusterConfig config = mongo.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found " ).build();
        }
        String cluster = JsonUtil.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response configureCluster( final String config )
    {
        TrimmedMongodbConfig trimmedConfig = JsonUtil.fromJson( config, TrimmedMongodbConfig.class );
        MongoClusterConfig mongoConfig = mongo.newMongoClusterConfigInstance();
        mongoConfig.setDomainName( trimmedConfig.getDomainName() );
        mongoConfig.setReplicaSetName( trimmedConfig.getReplicaSetName() );
        mongoConfig.setRouterPort( trimmedConfig.getRouterPort() );
        mongoConfig.setDataNodePort( trimmedConfig.getDataNodePort() );
        mongoConfig.setCfgSrvPort( trimmedConfig.getCfgSrvPort() );
        mongoConfig.setEnvironmentId( trimmedConfig.getEnvironmentId() );
        mongoConfig.setClusterName( trimmedConfig.getClusterName() );

        if ( !CollectionUtil.isCollectionEmpty( trimmedConfig.getConfigNodes() ) )
        {
            mongoConfig.setConfigHosts( trimmedConfig.getConfigNodes() );
        }

        if ( !CollectionUtil.isCollectionEmpty( trimmedConfig.getDataNodes() ) )
        {
            mongoConfig.setDataHosts( trimmedConfig.getDataNodes() );
        }

        if ( !CollectionUtil.isCollectionEmpty( trimmedConfig.getRouterNodes() ) )
        {
            mongoConfig.setRouterHosts( trimmedConfig.getRouterNodes() );
        }
        UUID uuid = mongo.installCluster( mongoConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = mongo.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostname, String nodeType )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        NodeType type = null;
        if ( nodeType.contains( "config" ) )
        {
            type = NodeType.CONFIG_NODE;
        }
        else if ( nodeType.contains( "data" ) )
        {
            type = NodeType.DATA_NODE;
        }
        else if ( nodeType.contains( "router" ) )
        {
            type = NodeType.ROUTER_NODE;
        }
        UUID uuid = mongo.startNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }

	@Override
	public Response startNodes (String clusterName, String lxcHosts)
	{
		Preconditions.checkNotNull( clusterName );
		Preconditions.checkNotNull( lxcHosts );
		JSONArray arr = new JSONArray (lxcHosts);
		List <String> names = new ArrayList<>();
		List <String> types = new ArrayList<>();
		for (int i = 0; i < arr.length (); ++i)
		{
			JSONObject obj = arr.getJSONObject (i);
			String name = obj.getString ("name");
			names.add (name);
			String type = obj.getString ("type");
			types.add (type);
		}

		if( names == null || names.isEmpty() )
		{
			return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
		}

		int errors = 0;

		for( int i = 0; i < names.size(); ++i )
		{
			NodeType type = null;
			if ( types.get(i).contains( "config" ) )
			{
				type = NodeType.CONFIG_NODE;
			}
			else if ( types.get(i).contains( "data" ) )
			{
				type = NodeType.DATA_NODE;
			}
			else if ( types.get(i).contains( "router" ) )
			{
				type = NodeType.ROUTER_NODE;
			}
			UUID uuid = mongo.startNode ( clusterName, names.get (i), type );
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
    public Response stopNode( final String clusterName, final String lxcHostname, String nodeType )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        NodeType type = null;
        if ( nodeType.contains( "config" ) )
        {
            type = NodeType.CONFIG_NODE;
        }
        else if ( nodeType.contains( "data" ) )
        {
            type = NodeType.DATA_NODE;
        }
        else if ( nodeType.contains( "router" ) )
        {
            type = NodeType.ROUTER_NODE;
        }
        UUID uuid = mongo.stopNode( clusterName, lxcHostname, type );
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
		List <String> types = new ArrayList<>();
		for (int i = 0; i < arr.length (); ++i)
		{
			JSONObject obj = arr.getJSONObject (i);
			String name = obj.getString ("name");
			names.add (name);
			String type = obj.getString ("type");
			types.add (type);
		}

		if( names == null || names.isEmpty() )
		{
			return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
		}

		int errors = 0;

		for( int i = 0; i < names.size(); ++i )
		{
			NodeType type = null;
			if ( types.get(i).contains( "config" ) )
			{
				type = NodeType.CONFIG_NODE;
			}
			else if ( types.get(i).contains( "data" ) )
			{
				type = NodeType.DATA_NODE;
			}
			else if ( types.get(i).contains( "router" ) )
			{
				type = NodeType.ROUTER_NODE;
			}
			UUID uuid = mongo.stopNode ( clusterName, names.get (i), type );
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
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = mongo.startAllNodes( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = mongo.stopAllNodes( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        NodeType type = null;
        if ( nodeType.contains( "config" ) )
        {
            type = NodeType.CONFIG_NODE;
        }
        else if ( nodeType.contains( "data" ) )
        {
            type = NodeType.DATA_NODE;
        }
        else if ( nodeType.contains( "router" ) )
        {
            type = NodeType.ROUTER_NODE;
        }

        UUID uuid = mongo.destroyNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname, String nodeType )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        NodeType type = null;
        if ( nodeType.contains( "config" ) )
        {
            type = NodeType.CONFIG_NODE;
        }
        else if ( nodeType.contains( "data" ) )
        {
            type = NodeType.DATA_NODE;
        }
        else if ( nodeType.contains( "router" ) )
        {
            type = NodeType.ROUTER_NODE;
        }
        UUID uuid = mongo.checkNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String nodeType )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodeType );
        if ( mongo.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        NodeType type = null;
        if ( nodeType.contains( "config" ) )
        {
            type = NodeType.CONFIG_NODE;
        }
        else if ( nodeType.contains( "data" ) )
        {
            type = NodeType.DATA_NODE;
        }
        else if ( nodeType.contains( "router" ) )
        {
            type = NodeType.ROUTER_NODE;
        }
        UUID uuid = mongo.addNode( clusterName, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private MongoPojo updateConfig( MongoClusterConfig config )
    {
        MongoPojo pojo = new MongoPojo();
        Set<ContainerPojo> configHosts = Sets.newHashSet();
        Set<ContainerPojo> routerHosts = Sets.newHashSet();
        Set<ContainerPojo> dataHosts = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );

            // TODO check primary nodes
            // check Primary data, config node
//            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
//            Set<EnvironmentContainerHost> dataNodes = environment.getContainerHostsByIds( config.getDataHosts() );
//            Set<EnvironmentContainerHost> configServers = environment.getContainerHostsByIds( config.getConfigHosts() );
//
//            for ( final EnvironmentContainerHost dataNode : dataNodes )
//            {
//                CommandResult result = dataNode.execute( Commands.getCheckIsMaster( config.getDataNodePort() ) );
//
//                if ( result.getStdOut().contains( "\"ismaster\" : true" ) )
//                {
//                    config.setPrimaryDataNode( dataNode.getId() );
//                }
//            }
//
//            for ( final EnvironmentContainerHost configServer : configServers )
//            {
//                CommandResult result =
//                        configServer.execute( Commands.getCheckIsMaster( config.getCfgSrvPort() ) );
//
//                if ( result.getStdOut().contains( "\"ismaster\" : true" ) )
//                {
//                    config.setPrimaryDataNode( configServer.getId() );
//                }
//            }

            String envDataSource = env.toString().contains( "ProxyEnvironment" ) ? "hub" : "subutai";

            pojo.setEnvironmentDataSource( envDataSource );

            for ( final String uuid : config.getConfigHosts() )
            {
                ContainerHost ch = env.getContainerHostById( uuid );
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                UUID uuidStatus = mongo.checkNode( config.getClusterName(), ch.getHostname (), NodeType.CONFIG_NODE );
                configHosts.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp(),
                        checkStatus( tracker, uuidStatus ) ) );
            }
            pojo.setConfigHosts( configHosts );

            for ( final String uuid : config.getRouterHosts() )
            {
                ContainerHost ch = env.getContainerHostById( uuid );
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                UUID uuidStatus = mongo.checkNode( config.getClusterName(), ch.getHostname(), NodeType.ROUTER_NODE );
                routerHosts.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp(),
                        checkStatus( tracker, uuidStatus ) ) );
            }
            pojo.setRouterHosts( routerHosts );

            for ( final String uuid : config.getDataHosts() )
            {
                ContainerHost ch = env.getContainerHostById( uuid );
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                UUID uuidStatus = mongo.checkNode( config.getClusterName(), ch.getHostname(), NodeType.DATA_NODE );
                dataHosts.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp(),
                        checkStatus( tracker, uuidStatus ) ) );
            }
            pojo.setDataHosts( dataHosts );
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
            TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "service is NOT running on node" ) )
                    {
                        state = "STOPPED";
                    }
                    else if ( po.getLog().contains( "service is running on node" ) )
                    {
                        state = "RUNNING";
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
	public Response autoScaleCluster (String clusterName, boolean scale)
	{
		MongoClusterConfig config = mongo.getCluster( clusterName );
		config.setAutoScaling( scale );
		try
		{
			mongo.saveConfig( config );
		}
		catch ( ClusterException e )
		{
			e.printStackTrace();
		}


		return Response.ok().build();
	}

	@Override
	public Response installCluster (String config)
	{
		ClusterConfJson clusterConfJson = JsonUtil.fromJson( config, ClusterConfJson.class );

		MongoClusterConfig clusterConfig = new MongoClusterConfig( );

		clusterConfig.setClusterName( clusterConfJson.getName() );
		clusterConfig.setDomainName( clusterConfJson.getDomainName() );
		clusterConfig.setReplicaSetName ( clusterConfJson.getRepl () );
		clusterConfig.setCfgSrvPort ( Integer.parseInt (clusterConfJson.getConfigPort ()) );
		clusterConfig.setRouterPort ( Integer.parseInt (clusterConfJson.getRoutePort ()) );
		clusterConfig.setDataNodePort ( Integer.parseInt (clusterConfJson.getDataPort ()) );
		clusterConfig.setConfigHosts ( clusterConfJson.getConfigNodes () );
		clusterConfig.setRouterHosts ( clusterConfJson.getRouteNodes () );
		clusterConfig.setDataHosts ( clusterConfJson.getDataNodes () );

		clusterConfig.setNumberOfConfigServers ( clusterConfJson.getConfigNodes ().size() );
		clusterConfig.setNumberOfRouters ( clusterConfJson.getRouteNodes ().size() );
		clusterConfig.setNumberOfDataNodes (clusterConfJson.getDataNodes ().size() );
		clusterConfig.setEnvironmentId( clusterConfJson.getEnvironmentId() );

        UUID uuid = mongo.installCluster( clusterConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
	}


	private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( JsonUtil.toJson (po.getLog()) ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( JsonUtil.toJson (po.getLog()) ).build();
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
            TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, uuid );
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
    public Response getAngularConfig()
    {
        return Response.ok (mongo.getWebModule().getAngularDependecyList()).build();
    }
}
