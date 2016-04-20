package io.subutai.plugin.shark.impl;

import com.google.gson.Gson;
import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;

public class SharkWebModule implements WebuiModule
{
	public static String NAME = "Shark";
	public static String IMG = "plugins/shark/shark.png";

	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<>();
		TEMPLATES_REQUIREMENT.put("hadoop", 1);
	}


	private WebuiModuleResourse sharkResource;


	public void init()
	{
		sharkResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.shark",
				"plugins/shark/shark.js",
				"plugins/shark/controller.js",
				"plugins/shark/service.js",
				"plugins/hadoop/service.js",
				"plugins/spark/service.js",
				"subutai-app/environment/service.js"
		);

		sharkResource.addDependency(angularjsDependency);
	}

	@Override
	public String getAngularState()
	{
		return sharkResource.getAngularjsList();
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
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), sharkResource.getAngularjsList() );
	}
}
