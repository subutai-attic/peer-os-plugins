package io.subutai.plugin.generic.impl;


import java.util.List;

import io.subutai.common.dao.DaoManager;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.impl.dao.ConfigDataServiceImpl;


public class GenericPluginImpl implements GenericPlugin
{
    private DaoManager daoManager;
    private ConfigDataService configDataService;
    private ConfigManager configManager;


    public GenericPluginImpl( final DaoManager daoManager )
    {
        this.daoManager = daoManager;
    }


    public void init()
    {
        configDataService = new ConfigDataServiceImpl( daoManager );
        configManager = new ConfigManager();
    }


    @Override
    public void saveProfile( final String profileName )
    {
        configDataService.saveProfile( profileName );
    }


    @Override
    public List<io.subutai.plugin.generic.api.model.Profile> getProfiles()
    {
        return configDataService.getAllProfiles();
    }


    @Override
    public void saveOperation( final Long profileId, final String operationName, final String commandName,
                               final String cwd, final String timeout, final Boolean daemon )
    {
        configDataService.saveOperation( profileId, operationName, commandName, cwd, timeout, daemon );
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
    public void updateOperation( final Operation operation, final String commandValue, final String cwdValue,
                                 final String timeoutValue, final Boolean daemonValue )
    {
        configDataService.updateOperation( operation, commandValue, cwdValue, timeoutValue, daemonValue );
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


    public ConfigDataService getConfigDataService()
    {
        return configDataService;
    }
}
