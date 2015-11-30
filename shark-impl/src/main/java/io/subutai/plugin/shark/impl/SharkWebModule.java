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
		return ".state('shark', {\n"
				+ "url: '/plugins/shark',\n"
				+ "templateUrl: 'plugins/shark/partials/view.html',\n"
				+ "resolve: {\n"
					+ "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
						+ "return $ocLazyLoad.load([\n" + "{\n"
							+ "name: 'subutai.plugins.shark',\n"
						+ "files: [\n"
							+ "'plugins/shark/shark.js',\n"
							+ "'plugins/shark/controller.js',\n"
							+ "'plugins/shark/service.js',\n"
							+ "'plugins/hadoop/service.js',\n"
							+ "'plugins/spark/service.js',\n"
							+ "'subutai-app/environment/service.js'\n"
						+ "]\n"
					+ "}\n"
				+ "]);\n"
				+ "}]\n"
				+ "}\n"
				+ "})";
	}
}
