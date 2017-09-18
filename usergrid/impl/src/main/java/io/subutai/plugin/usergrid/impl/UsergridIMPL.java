/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.TrustManager;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cxf.configuration.jsse.TLSClientParameters;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.EnvironmentStatus;
import io.subutai.common.environment.NodeSchema;
import io.subutai.common.environment.Topology;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.EnvironmentId;
import io.subutai.common.peer.PeerException;
import io.subutai.common.protocol.CustomProxyConfig;
import io.subutai.common.security.crypto.ssl.NaiveTrustManager;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.api.UsergridInterface;
import io.subutai.plugin.usergrid.impl.handler.ClusterOperationHandler;


public class UsergridIMPL implements UsergridInterface, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( UsergridIMPL.class.getName() );
    private ExecutorService executor;
    private final PluginDAO pluginDAO;
    private Tracker tracker;
    private NetworkManager networkManager;
    private EnvironmentManager environmentManager;
    private PeerManager peerManager;
    private Environment environment;
    private UsergridConfig userGridConfig;
    private String token;


    private static final String BUILD_TOPOLOGY_URL = "https://localhost:8443/rest/v1/strategy/ROUND-ROBIN-STRATEGY";
    private static final String ENVIRONMENT_URL = "https://localhost:8443/rest/v1/environments/";


    public UsergridIMPL( PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {

    }


    @Override
    public List<String> getClusterList( Environment name )
    {
        List<String> c = new ArrayList();
        Set<EnvironmentContainerHost> containerHosts = name.getContainerHosts();
        for ( EnvironmentContainerHost containerHost : containerHosts )
        {
            c.add( containerHost.getHostname() );
        }
        return c;
    }


    @Override
    public UUID installCluster( UsergridConfig usergridConfig )
    {
        LOG.info( "Install cluster Started..." );
        Preconditions.checkNotNull( usergridConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( usergridConfig.getClusterName() ),
                "clusterName is empty or null" );
        AbstractOperationHandler abstractOperationHandler =
                new ClusterOperationHandler( this, usergridConfig, ClusterOperationType.INSTALL );
        executor.execute( abstractOperationHandler );
        return abstractOperationHandler.getTrackerId();
    }


    private WebClient createWebClient( String url, Boolean trustCerts )
    {
        JacksonJsonProvider jsonProvider = new JacksonJsonProvider();
        WebClient webClient = WebClient.create( url, Collections.singletonList( jsonProvider ) );
        if ( trustCerts )
        {
            HTTPConduit conduit = WebClient.getConfig( webClient ).getHttpConduit();
            TLSClientParameters params = conduit.getTlsClientParameters();
            if ( params == null )
            {
                params = new TLSClientParameters();
                conduit.setTlsClientParameters( params );
            }
            params.setTrustManagers( new TrustManager[] {
                    new NaiveTrustManager()
            } );
            params.setDisableCNCheck( true );
        }
        return webClient;
    }


    private UsergridConfig buildEnvironment( UsergridConfig lConfig )
    {
        LOG.info( "building environment started" );

        String environmentName = lConfig.getEnvironmentName();
        //        NodeSchema elasticNode =
        //                new NodeSchema( "elasticsearch144" + randomAlphabetic( 10 ).toLowerCase(), ContainerSize.HUGE,
        //                        "elasticsearch144", 0, 0 );
        //        NodeSchema cassandraNode =
        //                new NodeSchema( "cassandra" + randomAlphabetic( 10 ).toLowerCase(), ContainerSize.HUGE,
        // "cassandra", 0,
        //                        0 );
        //        NodeSchema tomcatNode =
        //                new NodeSchema( "tomcat7" + randomAlphabetic( 10 ).toLowerCase(), ContainerSize.HUGE,
        // "tomcat7", 0, 0 );
        List<NodeSchema> nodes = new ArrayList<>();
        //        nodes.add( tomcatNode );
        //        nodes.add( cassandraNode );
        //        nodes.add( elasticNode );
        Blueprint blueprint = new Blueprint( environmentName, nodes );
        Topology topology = buildTopology( blueprint );
        LOG.info( "topology: " + blueprint.toString() );
        EnvironmentId usergridEnvironmentID = createEnvironment( topology );
        Boolean healt = false;
        while ( !healt )
        {
            try
            {
                TimeUnit.SECONDS.sleep( 10 );
                Environment env = environmentManager.loadEnvironment( usergridEnvironmentID.getId() );
                if ( env != null && env.getStatus().equals( EnvironmentStatus.HEALTHY ) )
                {
                    LOG.info( "Environment loaded and healty..." );
                    List<String> cassList = new ArrayList();
                    List<String> elasticList = new ArrayList();
                    Set<EnvironmentContainerHost> containerHosts = env.getContainerHosts();
                    for ( EnvironmentContainerHost e : containerHosts )
                    {
                        switch ( e.getTemplateName() )
                        {
                            case "elasticsearch144":
                            {
                                elasticList.add( e.getHostname() );
                                break;
                            }
                            case "cassandra":
                            {
                                cassList.add( e.getHostname() );
                                break;
                            }
                            case "tomcat7":
                            {
                                lConfig.setClusterName( e.getHostname() );
                                lConfig.setTomcatName( e.getHostname() );
                                break;
                            }
                        }
                    }
                    lConfig.setElasticSName( elasticList );
                    lConfig.setCassandraName( cassList );
                    lConfig.setEnvironmentId( usergridEnvironmentID.getId() );
                    healt = true;
                }
            }
            catch ( EnvironmentNotFoundException | InterruptedException ex )
            {

                LOG.error( "environment can not loaded yet..." + ex );
            }
        }

        return lConfig;
    }


    private EnvironmentId createEnvironment( Topology topology )
    {
        LOG.info( "create environment started" );
        WebClient webClient = createWebClient( ENVIRONMENT_URL, true );
        webClient.type( MediaType.APPLICATION_JSON );
        webClient.accept( MediaType.APPLICATION_JSON );
        webClient.replaceHeader( "sptoken", token );
        LOG.info( webClient.getHeaders().toString() );
        Response response = webClient.post( topology );
        LOG.info( String.valueOf( response.getStatus() ) );
        if ( response.getStatus() == 200 )
        {
            return response.readEntity( EnvironmentId.class );
        }
        else
        {
            return null;
        }
    }


    private Topology buildTopology( Blueprint blueprint )
    {
        WebClient webClient = createWebClient( BUILD_TOPOLOGY_URL, true );
        webClient.type( MediaType.APPLICATION_JSON );
        webClient.accept( MediaType.APPLICATION_JSON );
        webClient.replaceHeader( "sptoken", token );
        LOG.info( webClient.getHeaders().toString() );
        Response response = webClient.post( blueprint );
        LOG.info( String.valueOf( response.getStatus() ) );
        if ( response.getStatus() == 200 )
        {
            return response.readEntity( Topology.class );
        }
        else
        {
            return null;
        }
    }


    @Override
    public UUID oneClickInstall( UsergridConfig localConfig )
    {
        LOG.info( "one click install" );
        UUID uuid = null;
        token = localConfig.getPermanentToken();
        UsergridConfig newLocalConfig = buildEnvironment( localConfig );
        if ( newLocalConfig.getClusterName() != null )
        {
            AbstractOperationHandler abstractOperationHandler =
                    new ClusterOperationHandler( this, newLocalConfig, ClusterOperationType.INSTALL );
            executor.execute( abstractOperationHandler );
            uuid = abstractOperationHandler.getTrackerId();
        }
        return uuid;
    }


    private String getIPAddress( EnvironmentContainerHost ch )
    {
        String ipaddr = null;
        try
        {

            String localCommand = "ip addr | grep eth0 | grep \"inet\" | cut -d\" \" -f6 | cut -d\"/\" -f1";
            CommandResult resultAddr = ch.execute( new RequestBuilder( localCommand ) );
            ipaddr = resultAddr.getStdOut();
            ipaddr = ipaddr.replace( "\n", "" );
            LOG.info( "Container IP: " + ipaddr );
        }
        catch ( CommandException ex )
        {
            LOG.error( "ip address command error : " + ex );
        }
        return ipaddr;
    }


    @Override
    public void saveConfig( UsergridConfig ac ) throws ClusterException
    {
        if ( !getPluginDAO().saveInfo( UsergridConfig.PRODUCT_KEY, ac.getClusterName(), ac ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public UsergridConfig getConfig( String clusterName )
    {
        return this.userGridConfig;
    }


    @Override
    public UUID startCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID stopCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID restartCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID statusCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID startService( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID stopService( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID statusService( String clusterName, String hostName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID addNode( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public UUID uninstallCluster( String string )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public List<UsergridConfig> getClusters()
    {
        return this.pluginDAO.getInfo( UsergridConfig.PRODUCT_NAME, UsergridConfig.class );
    }


    @Override
    public UsergridConfig getCluster( String string )
    {
        return pluginDAO.getInfo( UsergridConfig.PACKAGE_NAME, string, UsergridConfig.class );
    }


    @Override
    public UUID addNode( String string, String string1 )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void onEnvironmentCreated( Environment e )
    {
        //not needed
    }


    @Override
    public void onEnvironmentGrown( Environment e, Set<EnvironmentContainerHost> set )
    {
        //not needed
    }


    @Override
    public void onContainerDestroyed( Environment e, String string )
    {
        String envId = e.getId();
        LOG.info( String.format( "Usergrid environment event: Environment destroyed: %s", envId ) );

        List<UsergridConfig> clusters = getClusters();
        for ( UsergridConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Usergrid environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                getPluginDAO().deleteInfo( UsergridConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                LOG.info( String.format( "Usergrid environment event: Cluster %s removed",
                        clusterConfig.getClusterName() ) );
                break;
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( String envId )
    {
        LOG.info( String.format( "Usergrid environment event: Environment destroyed: %s", envId ) );

        List<UsergridConfig> clusters = getClusters();
        for ( UsergridConfig clusterConfig : clusters )
        {
            if ( envId.equals( clusterConfig.getEnvironmentId() ) )
            {
                LOG.info( String.format( "Usergrid environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                getPluginDAO().deleteInfo( UsergridConfig.PRODUCT_KEY, clusterConfig.getClusterName() );

                try
                {
                    CustomProxyConfig proxyConfig =
                            new CustomProxyConfig( clusterConfig.getVlan(), clusterConfig.getClusterName(),
                                    clusterConfig.getEnvironmentId() );
                    peerManager.getPeer( clusterConfig.getPeerId() ).removeCustomProxy( proxyConfig );
                }
                catch ( PeerException e )
                {
                    LOG.error( "Failed to delete cluster information from database" );
                }

                LOG.info( String.format( "Usergrid environment event: Cluster %s removed",
                        clusterConfig.getClusterName() ) );
                break;
            }
        }
    }


    @Override
    public void onContainerStarted( final Environment environment, final String s )
    {

    }


    @Override
    public void onContainerStopped( final Environment environment, final String s )
    {

    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( ExecutorService executor )
    {
        this.executor = executor;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public NetworkManager getNetworkManager()
    {
        return networkManager;
    }


    public void setNetworkManager( NetworkManager networkManager )
    {
        this.networkManager = networkManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public Environment getEnvironment()
    {
        return environment;
    }


    public void setEnvironment( Environment environment )
    {
        this.environment = environment;
    }


    public UsergridConfig getUsergridConfig()
    {
        return userGridConfig;
    }


    public void setUsergridConfig( UsergridConfig usergridConfig )
    {
        this.userGridConfig = usergridConfig;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }
}

