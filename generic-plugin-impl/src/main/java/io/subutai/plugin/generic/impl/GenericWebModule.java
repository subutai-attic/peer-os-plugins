package io.subutai.plugin.generic.impl;

import io.subutai.webui.api.WebuiModule;

public class GenericWebModule implements WebuiModule
{
	public static String NAME = "Generic";
	public static String IMG = "plugins/generic/generic.png";

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
				+ "url: '/plugins/generic',"
				+ "templateUrl: 'plugins/generic/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.generic',"
				+ "files: ["
				+ "'plugins/generic/generic.js',"
				+ "'plugins/generic/controller.js',"
				+ "'plugins/generic/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
