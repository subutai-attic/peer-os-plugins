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
		return ".state('hive', {\n" + "url: '/plugins/hive',\n"
				+ "templateUrl: 'plugins/hive/partials/view.html',\n" + "resolve: {\n"
				+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
				+ "return $ocLazyLoad.load([\n" + "{\n"
				+ "name: 'subutai.plugins.hive',\n" + "files: [\n"
				+ "'plugins/hive/hive.js',\n" + "'plugins/hive/controller.js',\n"
				+ "'plugins/hive/service.js',\n" + "'plugins/hadoop/service.js',\n"
				+ "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
				+ "]);\n" + "}]\n" + "}\n" + "})";
	}
}
