package io.subutai.plugin.generic.impl;

import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.GenericPluginConfiguration;
import io.subutai.plugin.generic.api.Profile;


public class GenericPluginImpl implements GenericPlugin
{
	@Override
	public String executeCommandOnContainer (GenericPluginConfiguration config)
	{
		ExecutorManager exec = new ExecutorManager (config);
		return exec.execute();
	}


    @Override
    public void saveProfile( final Profile profile )
    {

    }


    @Override
    public Profile getProfile()
    {
        return null;
    }
}
