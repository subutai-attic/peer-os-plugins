package io.subutai.plugin.mongodb.impl;


import io.subutai.webui.api.WebuiModule;

public class MongoWebModule implements WebuiModule
{
	public static String NAME = "Mongo";
	public static String IMG = "plugins/mongo/mongo.png";

	public MongoWebModule()
	{

	}

	@Override
	public String getName()
	{
		return NAME;
	}


	@Override
	public String getModuleInfo()
	{
		return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
	}


	@Override
	public String getAngularDependecyList()
	{
		return ".state('mongo', {\n" +
				"url: '/plugins/mongo',\n" +
				"templateUrl: 'plugins/mongo/partials/view.html',\n" +
				"resolve: {\n" +
				"loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"return $ocLazyLoad.load([\n" +
				"{\n" +
				"name: 'subutai.plugins.mongo',\n" +
				"files: [\n" +
				"'plugins/mongo/mongo.js',\n" +
				"'plugins/mongo/controller.js',\n" +
				"'plugins/mongo/service.js',\n" +
				"'plugins/hadoop/service.js',\n" +
				"'subutai-app/environment/service.js'\n" +
				"]\n" +
				"}\n" +
				"]);\n" +
				"}]\n" +
				"}\n" +
				"})";
	}
}
