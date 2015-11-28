package io.subutai.plugin.mongodb.rest;


import java.util.*;

import javax.ws.rs.FormParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;

import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.CollectionUtil;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.mongodb.api.Mongo;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.NodeType;
import org.json.JSONArray;
import org.json.JSONObject;


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

		boolean thrownException = false;
		if ( config == null )
		{
			thrownException = true;
		}


		Map< String, ContainerInfoJson > map = new HashMap<> (  );
		for( String node : config.getConfigHosts () )
		{
			try
			{
				ContainerInfoJson containerInfoJson = new ContainerInfoJson();

				Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
				EnvironmentContainerHost containerHost = environment.getContainerHostById( node );

				String ip = containerHost.getIpByInterfaceName( "eth0" );
				containerInfoJson.setIp( ip );

				UUID uuid = mongo.checkNode( clusterName, node, NodeType.CONFIG_NODE );
				OperationState state = waitUntilOperationFinish( uuid );
				Response response = createResponse( uuid, state );
				if( response.getStatus() == 200 && !response.getEntity().toString().toUpperCase().contains( "NOT" ) )
				{
					containerInfoJson.setStatus( "RUNNING" );
				}
				else
				{
					containerInfoJson.setStatus( "STOPPED" );
				}

				map.put( node, containerInfoJson );
			}
			catch ( Exception e )
			{
				map.put( node, new ContainerInfoJson() );
				thrownException = true;
			}
		}
		for( String node : config.getRouterHosts () )
		{
			try
			{
				ContainerInfoJson containerInfoJson = new ContainerInfoJson();

				Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
				EnvironmentContainerHost containerHost = environment.getContainerHostById( node );

				String ip = containerHost.getIpByInterfaceName( "eth0" );
				containerInfoJson.setIp( ip );

				UUID uuid = mongo.checkNode( clusterName, node, NodeType.ROUTER_NODE );
				OperationState state = waitUntilOperationFinish( uuid );
				Response response = createResponse( uuid, state );
				if( response.getStatus() == 200 && !response.getEntity().toString().toUpperCase().contains( "NOT" ) )
				{
					containerInfoJson.setStatus( "RUNNING" );
				}
				else
				{
					containerInfoJson.setStatus( "STOPPED" );
				}

				map.put( node, containerInfoJson );
			}
			catch ( Exception e )
			{
				map.put( node, new ContainerInfoJson() );
				thrownException = true;
			}
		}
		for( String node : config.getDataHosts () )
		{
			try
			{
				ContainerInfoJson containerInfoJson = new ContainerInfoJson();

				Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
				EnvironmentContainerHost containerHost = environment.getContainerHostById( node );

				String ip = containerHost.getIpByInterfaceName( "eth0" );
				containerInfoJson.setIp( ip );

				UUID uuid = mongo.checkNode( clusterName, node, NodeType.DATA_NODE );
				OperationState state = waitUntilOperationFinish( uuid );
				Response response = createResponse( uuid, state );
				if( response.getStatus() == 200 && !response.getEntity().toString().toUpperCase().contains( "NOT" ) )
				{
					containerInfoJson.setStatus( "RUNNING" );
				}
				else
				{
					containerInfoJson.setStatus( "STOPPED" );
				}

				map.put( node, containerInfoJson );
			}
			catch ( Exception e )
			{
				map.put( node, new ContainerInfoJson() );
				thrownException = true;
			}
		}

		ClusterConfJson result = new ClusterConfJson();
		result.setConfigPort (String.valueOf (config.getCfgSrvPort ()));
		result.setRoutePort (String.valueOf (config.getRouterPort ()));
		result.setDataPort (String.valueOf (config.getDataNodePort ()));
		result.setConfigNodes (config.getConfigHosts ());
		result.setRouteNodes (config.getRouterHosts ());
		result.setDataNodes (config.getDataHosts ());
		result.setContainersStatuses( map );
		result.setName( config.getClusterName() );
		result.setScaling( config.isAutoScaling() );

		String cluster = JsonUtil.toJson( result );




		if( thrownException )
		{
			return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
					.entity( clusterName + " cluster not found." ).build();
		}
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

		Preconditions.checkNotNull( clusterName );
		Preconditions.checkNotNull( lxcHosts );

		List<String> hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>(){}.getType() );

		if( hosts == null || hosts.isEmpty() )
		{
			return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
		}

		int errors = 0;

		for( String host : hosts )
		{
			UUID uuid = mongo.stopService( clusterName, host );
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

		mongo.installCluster( clusterConfig );
		return Response.ok().build();
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
}
