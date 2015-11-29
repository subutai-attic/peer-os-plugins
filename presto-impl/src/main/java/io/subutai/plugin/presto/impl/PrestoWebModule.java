package io.subutai.plugin.presto.impl;

import io.subutai.webui.api.WebuiModule;

public class PrestoWebModule implements WebuiModule
{
	public static String NAME = "Presto";
	public static String IMG = "plugins/presto/presto.png";

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
				+ "url: '/plugins/presto',"
				+ "templateUrl: 'plugins/presto/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.presto',"
				+ "files: ["
				+ "'plugins/presto/presto.js',"
				+ "'plugins/presto/controller.js',"
				+ "'plugins/presto/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
