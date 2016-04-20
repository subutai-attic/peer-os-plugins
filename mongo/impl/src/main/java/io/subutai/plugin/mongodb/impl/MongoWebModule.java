package io.subutai.plugin.mongodb.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class MongoWebModule implements WebuiModule
{
	private WebuiModuleResourse mongoResource;
	public static String NAME = "Mongo";
	public static String IMG = "plugins/mongo/mongo.png";
	private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
	static
	{
		TEMPLATES_REQUIREMENT = new HashMap<>();
		TEMPLATES_REQUIREMENT.put("mongo", 1);
	}


	public void init()
	{
		this.mongoResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
		AngularjsDependency angularjsDependency = new AngularjsDependency(
				"subutai.plugins.mongo",
				"plugins/mongo/mongo.js",
				"plugins/mongo/controller.js",
				"plugins/mongo/service.js",
				"subutai-app/environment/service.js"
		);

		this.mongoResource.addDependency(angularjsDependency);
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
		return this.mongoResource.getAngularjsList();
	}


	@Override
	public String getAngularDependecyList()
	{
		return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.mongoResource.getAngularjsList() );
	}
}
