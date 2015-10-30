package io.subutai.plugin.generic.api;


public interface GenericPlugin
{
	String executeCommandOnContainer (GenericPluginConfiguration config);
	public void executeCommandOnContainer (GenericPluginConfiguration config);

    public void saveProfile( Profile profile );

    public Profile getProfile();
}
