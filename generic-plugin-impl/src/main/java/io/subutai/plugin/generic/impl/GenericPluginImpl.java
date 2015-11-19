package io.subutai.plugin.generic.impl;


import java.util.List;

import io.subutai.common.dao.DaoManager;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.impl.dao.ConfigDataServiceImpl;


public class GenericPluginImpl implements GenericPlugin
{
    private DaoManager daoManager;


    @Override
    public ConfigDataService getConfigDataService()
    {
        return configDataService;
    }


    @Override
    public void deleteProfile( final Long profileId )
    {
        configDataService.deleteProfile( profileId );
    }


    @Override
    public void deleteOperations( final Long profileId )
    {
        configDataService.deleteOperations( profileId );
    }


    private ConfigDataService configDataService;


    public GenericPluginImpl( final DaoManager daoManager )
    {
        this.daoManager = daoManager;
    }


    public void init()
    {
        configDataService = new ConfigDataServiceImpl( daoManager );
    }


    @Override
    public void saveProfile( final String profileName )
    {
        configDataService.saveProfile( profileName );
    }


    @Override
    public List<Profile> getProfiles()
    {
        return configDataService.getAllProfiles();
    }


    @Override
    public void saveOperation( final Long profileId, final String operationName, final String commandName,
                               final String cwd, final String timeout, final Boolean daemon, final Boolean fromFile )
    {
        configDataService.saveOperation( profileId, operationName, commandName, cwd, timeout, daemon, fromFile );
    }


    @Override
    public List<Operation> getProfileOperations( final Long profileId )
    {
        return configDataService.getOperations( profileId );
    }


    @Override
    public boolean IsOperationRegistered( final String operationName )
    {
        return configDataService.isOperationRegistered( operationName );
    }


    @Override
    public void updateOperation( final Long operationId, final String commandValue, final String cwdValue,
                                 final String timeoutValue, final Boolean daemonValue, final Boolean fromFile )
    {
        configDataService.updateOperation( operationId, commandValue, cwdValue, timeoutValue, daemonValue, fromFile );
    }


    @Override
    public void deleteOperation( final Long operationId )
    {
        configDataService.deleteOperation( operationId );
    }


    @Override
    public String executeCommandOnContainer( final ContainerHost host, final Operation operation )
    {
        ExecutorManager manager = new ExecutorManager( host, operation );
        return manager.execute();
    }
}
