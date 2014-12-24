package org.safehaus.subutai.plugin.oozie.rest;


import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.Oozie;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.SetupType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;


public class RestServiceImpl implements RestService
{

    private Oozie oozieManager;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Oozie oozieManager, final Hadoop hadoopManager,
                            final EnvironmentManager environmentManager )
    {
        Preconditions.checkNotNull( oozieManager );
        Preconditions.checkNotNull( hadoopManager );
        Preconditions.checkNotNull( environmentManager );

        this.oozieManager = oozieManager;
        this.hadoopManager = hadoopManager;
        this.environmentManager = environmentManager;
    }


    @Override
    public Response listClusters()
    {
        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response createCluster( final String config )
    {

        TrimmedOozieClusterConfig tocc = JsonUtil.fromJson( config, TrimmedOozieClusterConfig.class );

        if ( Strings.isNullOrEmpty( tocc.getClusterName() ) )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( "Invalid cluster name" ).build();
        }

        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( tocc.getHadoopClusterName() );


        if ( hadoopClusterConfig == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( String.format( "Hadoop cluster %s not found", tocc.getHadoopClusterName() ) )
                           .build();
        }

        Environment environment = environmentManager.getEnvironmentByUUID( hadoopClusterConfig.getEnvironmentId() );

        if ( environment == null )
        {
            return Response.status( Response.Status.NOT_FOUND ).entity(
                    String.format( "Environment %s not found", hadoopClusterConfig.getEnvironmentId() ) ).build();
        }

        ContainerHost serverHost = environment.getContainerHostByHostname( tocc.getServerHostname() );
        if ( serverHost == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( String.format( "Server node %s not found", tocc.getServerHostname() ) ).build();
        }

        Set<UUID> clientHosts = Sets.newHashSet();
        for ( String clientHostName : tocc.getClientHostNames() )
        {
            ContainerHost clientHost = environment.getContainerHostByHostname( clientHostName );
            if ( clientHost == null )
            {
                return Response.status( Response.Status.NOT_FOUND )
                               .entity( String.format( "Client node %s not found", clientHostName ) ).build();
            }
            clientHosts.add( clientHost.getId() );
        }

        OozieClusterConfig occ = new OozieClusterConfig();
        occ.setClusterName( tocc.getClusterName() );
        occ.setHadoopClusterName( tocc.getHadoopClusterName() );
        occ.setServer( serverHost.getId() );
        occ.setSetupType( SetupType.OVER_HADOOP );
        occ.setClients( clientHosts );

        UUID uuid = this.oozieManager.installCluster( occ );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        UUID uuid = oozieManager.uninstallCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        OozieClusterConfig occ = oozieManager.getCluster( clusterName );
        UUID uuid = oozieManager.startServer( occ );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        OozieClusterConfig occ = oozieManager.getCluster( clusterName );
        UUID uuid = oozieManager.stopServer( occ );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        UUID uuid = oozieManager.addNode( clusterName, lxcHostname, nodeType );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname )
    {
        UUID uuid = oozieManager.destroyNode( clusterName, lxcHostname );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname )
    {
        OozieClusterConfig occ = oozieManager.getCluster( clusterName );
        UUID uuid = oozieManager.checkServerStatus( occ );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    private String wrapUUID( UUID uuid )
    {
        return JsonUtil.toJson( "OPERATION_ID", uuid );
    }
}
