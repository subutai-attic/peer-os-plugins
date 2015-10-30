package io.subutai.plugin.generic.api;


import java.util.List;


public interface GenericPlugin
{
	String executeCommandOnContainer (GenericPluginConfiguration config);

    public void saveProfile( Profile profile );

    public List<Profile> getProfiles();
}
