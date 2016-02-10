package io.subutai.plugin.bazaar.api.dao;


import io.subutai.plugin.bazaar.api.model.Plugin;

import java.util.List;

public interface ConfigDataService
{
	void savePlugin (final String name, final String version, final String kar, final String url);

	void deletePlugin (final Long id);

	List<Plugin> getPlugins();
}
