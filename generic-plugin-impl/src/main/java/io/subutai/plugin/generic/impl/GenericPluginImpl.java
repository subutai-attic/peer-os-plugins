package io.subutai.plugin.generic.impl;

import io.subutai.plugin.generic.api.GenericPluginConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.subutai.plugin.generic.api.GenericPlugin;

public class GenericPluginImpl implements GenericPlugin
{
	@Override
	public String executeCommandOnContainer (GenericPluginConfiguration config)
	{
		ExecutorManager exec = new ExecutorManager (config);
		return exec.execute();
	}
}
