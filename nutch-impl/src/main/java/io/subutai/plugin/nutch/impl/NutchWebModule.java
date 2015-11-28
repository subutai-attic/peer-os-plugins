package io.subutai.plugin.nutch.impl;


import io.subutai.webui.api.WebuiModule;

public class NutchWebModule implements WebuiModule
{
	public static String NAME = "Nutch";
	public static String IMG = "plugins/nutch/nutch.png";

	public NutchWebModule ()
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
		return "{"
				+ "url: '/plugins/nutch',"
				+ "templateUrl: 'plugins/nutch/partials/view.html',"
				+ "resolve: {"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
				+ "{"
				+ "name: 'subutai.plugins.nutch',"
				+ "files: ["
				+ "'plugins/nutch/nutch.js',"
				+ "'plugins/nutch/controller.js',"
				+ "'plugins/nutch/service.js'"
				+ "]"
				+ "}"
				+ "]);"
				+ "}]"
				+ "}";
	}
}
