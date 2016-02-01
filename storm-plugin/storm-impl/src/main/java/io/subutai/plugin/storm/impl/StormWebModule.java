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
		return ".state('storm', {\n" +
				"url: '/plugins/storm',\n" +
				"templateUrl: 'plugins/storm/partials/view.html',\n" +
				"data: {\n" +
				"bodyClass: '',\n" +
				"layout: 'default'\n" +
				"},\n" +
				"resolve: {\n" +
				"loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"return $ocLazyLoad.load([\n" +
				"{\n" +
				"name: 'subutai.plugins.storm',\n" +
				"files: [\n" +
				"'plugins/storm/storm.js',\n" +
				"'plugins/storm/controller.js',\n" +
				"'plugins/storm/service.js',\n" +
				"'subutai-app/environment/service.js'\n" +
				"]\n" +
				"}\n" +
				"]);\n" +
				"}]\n" +
				"}\n" +
				"})";
	}
}
