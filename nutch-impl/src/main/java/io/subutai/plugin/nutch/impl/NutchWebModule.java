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
		return ".state('nutch', {\n" +
				"url: '/plugins/nutch',\n" +
				"templateUrl: 'plugins/nutch/partials/view.html',\n" +
				"resolve: {\n" +
				"loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"return $ocLazyLoad.load([\n" +
				"{\n" +
				"name: 'subutai.plugins.nutch',\n" +
				"files: [\n" +
				"'plugins/nutch/nutch.js',\n" +
				"'plugins/nutch/controller.js',\n" +
				"'plugins/nutch/service.js',\n" +
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
