package io.subutai.plugin.hive.impl;

import io.subutai.webui.api.WebuiModule;

public class HiveWebModule implements WebuiModule
{
	public static String NAME = "Hive";
	public static String IMG = "plugins/hive/hive.png";

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
				+ "url: '/plugins/hive',"
				+ "templateUrl: 'plugins/hive/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.hive',"
				+ "files: ["
				+ "'plugins/hive/hive.js',"
				+ "'plugins/hive/controller.js',"
				+ "'plugins/hive/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
