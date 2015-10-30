package io.subutai.plugin.generic.impl;


import java.util.List;

import io.subutai.common.dao.DaoManager;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.GenericPluginConfiguration;
import io.subutai.plugin.generic.api.Profile;
import io.subutai.plugin.generic.api.dao.ConfigDataService;
import io.subutai.plugin.generic.impl.dao.ConfigDataServiceImpl;


public class GenericPluginImpl implements GenericPlugin
{
    private DaoManager daoManager;


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
    public String executeCommandOnContainer( GenericPluginConfiguration config )
    {
        ExecutorManager exec = new ExecutorManager( config );
        return exec.execute();
    }


    @Override
    public void saveProfile( final Profile profile )
    {
    }


    @Override
    public List<Profile> getProfiles()
    {
        return null;
    }

    public ConfigDataService getConfigDataService()
    {
        return configDataService;
    }

}
