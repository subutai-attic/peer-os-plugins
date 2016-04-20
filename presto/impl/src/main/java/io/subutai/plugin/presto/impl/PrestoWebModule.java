package io.subutai.plugin.presto.impl;

import io.subutai.webui.api.WebuiModule;

public class PrestoWebModule implements WebuiModule
{
	public static String NAME = "Presto";
	public static String IMG = "plugins/presto/presto.png";

	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<>();
		TEMPLATES_REQUIREMENT.put("hadoop", 1);
	}


	private WebuiModuleResourse prestoResource;


	public void init()
	{
		prestoResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.presto",
				"plugins/presto/presto.js",
				"plugins/presto/controller.js",
				"plugins/presto/service.js",
				"plugins/hadoop/service.js",
				"subutai-app/environment/service.js";
		);

		prestoResource.addDependency(angularjsDependency);
	}

	@Override
	public String getAngularState()
	{
		return prestoResource.getAngularjsList();
	}

	@Override
	public String getName()
	{
		return NAME;
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString());
	}


	@Override
	public String getAngularDependecyList()
	{
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), prestoResource.getAngularjsList() );
	}
}
