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
		return ".state('presto', {\n"
				+ "url: '/plugins/presto',\n"
				+ "templateUrl: 'plugins/presto/partials/view.html',\n"
				+ "resolve: {\n"
					+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
						+ "return $ocLazyLoad.load([\n" + "{\n"
							+ "name: 'subutai.plugins.presto',\n" + "files: [\n"
							+ "'plugins/presto/presto.js',\n" + "'plugins/presto/controller.js',\n"
							+ "'plugins/presto/service.js',\n" + "'plugins/hadoop/service.js',\n"
							+ "'subutai-app/environment/service.js'\n"
						+ "]\n"
					+ "}\n"
					+ "]);\n"
					+ "}]\n"
					+ "}\n"
				+ "})";
	}
}
