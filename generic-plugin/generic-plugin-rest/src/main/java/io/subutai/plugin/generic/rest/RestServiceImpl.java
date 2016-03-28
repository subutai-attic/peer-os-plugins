package io.subutai.plugin.generic.rest;


import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.rest.pojo.VersionPojo;


public class RestServiceImpl implements RestService
{
    private GenericPlugin genericPlugin;
    private EnvironmentManager environmentManager;
    private static final String ERROR_KEY = "ERROR";


    @Override
    public Response listProfiles()
    {
        List<Profile> profileList = genericPlugin.getProfiles();
        String profiles = JsonUtil.toJson( profileList );
        return Response.status( Response.Status.OK ).entity( profiles ).build();
    }


    @Override
    public Response saveProfile( final String profileName )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService.saveProfile( profileName );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response listOperations( final String profileId )
    {
        List<Operation> operationList = genericPlugin.getProfileOperations( Long.parseLong( profileId ) );
        String operations = JsonUtil.GSON.toJson( operationList );
        return Response.status( Response.Status.OK ).entity( operations ).build();
    }


    @Override
    public Response saveOperation( final String profileId, final String operationName, Attachment attr,
                                   final String cwd, final String timeout, final Boolean daemon, final Boolean script )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        String commandName;
        try
        {
            InputStream inputStream = attr.getDataHandler().getInputStream();
            StringWriter writer = new StringWriter();
            IOUtils.copy( inputStream, writer, "US-ASCII" );
            commandName = writer.toString();
        }
        catch ( IOException e )
        {
            return Response.serverError().entity( JsonUtil.toJson( ERROR_KEY, e.getMessage() ) ).build();
        }

        try
        {
            configDataService
                    .saveOperation( Long.parseLong( profileId ), operationName, commandName, cwd, timeout, daemon,
                            script );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response saveOperation( final String profileId, final String operationName, final String commandName,
                                   final String cwd, final String timeout, final Boolean daemon, final Boolean script )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService
                    .saveOperation( Long.parseLong( profileId ), operationName, commandName, cwd, timeout, daemon,
                            script );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response updateOperation( final String operationId, final String commandName, final String cwd,
                                     final String timeout, final Boolean daemon, final Boolean script,
                                     final String operaitonName )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService.updateOperation( Long.parseLong( operationId ), commandName, cwd, timeout, daemon, script,
                    operaitonName );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response updateOperation( final String operationId, final Attachment attr, final String cwd,
                                     final String timeout, final Boolean daemon, final Boolean script,
                                     final String operationName )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        String commandName;
        try
        {
            InputStream inputStream = attr.getDataHandler().getInputStream();
            StringWriter writer = new StringWriter();
            IOUtils.copy( inputStream, writer, "UTF-8" );
            commandName = writer.toString();
        }
        catch ( IOException e )
        {
            return Response.serverError().entity( JsonUtil.toJson( ERROR_KEY, e.getMessage() ) ).build();
        }

        try
        {
            configDataService.updateOperation( Long.parseLong( operationId ), commandName, cwd, timeout, daemon, script,
                    operationName );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response deleteOperation( final String operationId )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService.deleteOperation( Long.parseLong( operationId ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response deleteProfile( final String profileId )
    {
        try
        {
            ConfigDataService configDataService = genericPlugin.getConfigDataService();
            if ( configDataService.getOperations( Long.parseLong( profileId ) ).isEmpty()
                    || configDataService.getOperations( Long.parseLong( profileId ) ) == null )

            {
                configDataService.deleteProfile( Long.parseLong( profileId ) );
            }
            else
            {
                configDataService.deleteProfile( Long.parseLong( profileId ) );
                configDataService.deleteOperations( Long.parseLong( profileId ) );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    @Override
    public Response executeCommand( final String operationId, final String lxcHostName, final String environmentId )
    {
        ContainerHost host;
        Operation operation;
        String output;
        try
        {
            Environment environment = environmentManager.loadEnvironment( environmentId );
            host = environment.getContainerHostByHostname( lxcHostName );

            operation = genericPlugin.getConfigDataService().getOperationById( Long.parseLong( operationId ) );

            if ( operation != null )
            {
                output = genericPlugin.executeCommandOnContainer( host, operation );
            }
            else
            {
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
            }
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).entity( output ).build();
    }


    public void setGenericPlugin( final GenericPlugin genericPlugin )
    {
        this.genericPlugin = genericPlugin;
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
