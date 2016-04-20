package io.subutai.plugin.hive.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class HiveWebModule implements WebuiModule
{
	private WebuiModuleResourse hiveResource;
	public static String NAME = "Hive";
	public static String IMG = "plugins/hive/hive.png";
	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<>();
		TEMPLATES_REQUIREMENT.put("hadoop", 2);
	}


	public void init()
	{
		this.hiveResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.hive",
				"'plugins/hive/hive.js'",
				"'plugins/hive/controller.js'",
				"'plugins/hive/service.js'",
				"'plugins/hadoop/service.js'",
				"'subutai-app/environment/service.js'"
		);

		this.hiveResource.addDependency(angularjsDependency);
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
	}

	@Override
	public String getName()
	{
		return NAME;
	}

	@Override
	public String getAngularState()
	{
		return this.hiveResource.getAngularjsList();
	}


	@Override
	public String getAngularDependecyList()
	{
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.hiveResource.getAngularjsList() );
	}
}
