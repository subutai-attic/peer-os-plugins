package io.subutai.plugin.storm.impl;

import io.subutai.webui.api.WebuiModule;

public class StormWebModule implements WebuiModule
{
	public static String NAME = "Storm";
	public static String IMG = "plugins/storm/storm.png";

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
				+ "url: '/plugins/storm',"
				+ "templateUrl: 'plugins/storm/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.storm',"
				+ "files: ["
				+ "'plugins/storm/storm.js',"
				+ "'plugins/storm/controller.js',"
				+ "'plugins/storm/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
