package io.subutai.plugin.generic.impl;


import java.util.List;

import io.subutai.common.dao.DaoManager;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.impl.dao.ConfigDataServiceImpl;
import io.subutai.webui.api.WebuiModule;


public class GenericPluginImpl implements GenericPlugin
{
    private DaoManager daoManager;
    private GenericWebModule webModule;

    private ConfigDataService configDataService;


    public GenericPluginImpl( final DaoManager daoManager, final GenericWebModule webModule )
    {
        this.daoManager = daoManager;
        this.webModule = webModule;
    }


    @Override
    public ConfigDataService getConfigDataService()
    {
        return configDataService;
    }


    @Override
    public void deleteProfile( final String profileName )
    {
        configDataService.deleteProfile( profileName );
    }


    @Override
    public void deleteOperations( final Long profileId )
    {
        configDataService.deleteOperations( profileId );
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
    public void updateOperation( final Long operationId, final String commandValue, final String cwdValue,
                                 final String timeoutValue, final Boolean daemonValue, final Boolean fromFile,
                                 final String operationName )
    {
        configDataService.updateOperation( operationId, commandValue, cwdValue, timeoutValue, daemonValue, fromFile,
                operationName );
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

    @Override
    public WebuiModule getWebModule()
    {
        return webModule;
    }


    @Override
    public void setWebModule( final WebuiModule webModule )
    {
        this.webModule = (GenericWebModule) webModule;
    }
}
