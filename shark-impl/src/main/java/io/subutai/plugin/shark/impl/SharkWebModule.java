package io.subutai.plugin.shark.impl;

import io.subutai.webui.api.WebuiModule;

public class SharkWebModule implements WebuiModule
{
	public static String NAME = "Shark";
	public static String IMG = "plugins/shark/shark.png";

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
				+ "url: '/plugins/shark',"
				+ "templateUrl: 'plugins/shark/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.shark',"
				+ "files: ["
				+ "'plugins/shark/shark.js',"
				+ "'plugins/shark/controller.js',"
				+ "'plugins/shark/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
