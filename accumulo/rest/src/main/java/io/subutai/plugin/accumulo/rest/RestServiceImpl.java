package io.subutai.plugin.accumulo.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.ws.rs.FormParam;
import javax.ws.rs.core.Response;

import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.accumulo.api.Accumulo;
import io.subutai.plugin.accumulo.rest.pojo.VersionDto;
import io.subutai.plugin.hadoop.api.Hadoop;


public class RestServiceImpl implements RestService
{
    private Accumulo accumuloManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;


    public RestServiceImpl( final Accumulo accumuloManager, final Tracker tracker,
                            final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {
        this.accumuloManager = accumuloManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }


    @Override
    public Response listClusters()
    {
        return null;
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName, final String master,
                                    final String slaves )
    {
        return null;
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public Response addSlaveNode( final String clusterName, final String lxcHostName )
    {
        return null;
    }


    @Override
    public Response destroySlaveNode( final String clusterName, final String lxcHostName )
    {
        return null;
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        return null;
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        return null;
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        return null;
    }


    @Override
    public Response startNodes( final String clusterName, final String lxcHosts )
    {
        return null;
    }


    @Override
    public Response stopNodes( final String clusterName, final String lxcHosts )
    {
        return null;
    }


    @Override
    public Response getPluginInfo()
    {
        Properties prop = new Properties();
        VersionDto pojo = new VersionDto();
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
}
