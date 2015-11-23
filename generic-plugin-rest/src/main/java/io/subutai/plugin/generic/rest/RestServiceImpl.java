package io.subutai.plugin.generic.rest;


import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import io.subutai.common.util.JsonUtil;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


/**
 * Created by ermek on 11/10/15.
 */
public class RestServiceImpl implements RestService
{
    private GenericPlugin genericPlugin;


    @Override
    public Response listProfiles()
    {
        List<Profile> profileList = genericPlugin.getProfiles();
        ArrayList<String> profileNames = new ArrayList<>();

        for ( final Profile profile : profileList )
        {
            profileNames.add( profile.getName() );
        }

        String profiles = JsonUtil.toJson( profileNames );
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
        ArrayList<String> operationNames = new ArrayList<>();

        for ( final Operation operation : operationList )
        {
            operationNames.add( operation.getOperationName() );
        }

        String operations = JsonUtil.GSON.toJson( operationNames );
        return Response.status( Response.Status.OK ).entity( operations ).build();
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
                                     final String timeout, final Boolean daemon, final Boolean script, final String operaitonName )
    {
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService
                    .updateOperation( Long.parseLong( operationId ), commandName, cwd, timeout, daemon, script, operaitonName );
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
        ConfigDataService configDataService = genericPlugin.getConfigDataService();
        try
        {
            configDataService.deleteProfile( Long.parseLong( profileId ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }

        return Response.status( Response.Status.OK ).build();
    }


    public void setGenericPlugin( final GenericPlugin genericPlugin )
    {
        this.genericPlugin = genericPlugin;
    }
}
