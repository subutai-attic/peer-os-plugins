package io.subutai.plugin.mongodb.impl.handler;

import io.subutai.webui.api.WebuiModule;

public class MongoWebModule implements WebuiModule
{
	public static String NAME = "Cassandra";
	public static String IMG = "plugins/cassandra/cassandra.png";

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
		return "{"
				+ "url: '/plugins/mongo',"
				+ "templateUrl: 'plugins/mongo/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.mongo',"
				+ "files: ["
				+ "'plugins/mongo/mongo.js',"
				+ "'plugins/mongo/controller.js',"
				+ "'plugins/mongo/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
