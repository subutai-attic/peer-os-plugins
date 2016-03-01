package io.subutai.plugin.mahout.impl;


import io.subutai.webui.api.WebuiModule;


public class MahoutWebModule implements WebuiModule
{
	public static String NAME = "Mahout";
	public static String IMG = "plugins/mahout/mahout.png";

	public MahoutWebModule()
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
		return ".state('mahout', {\n" + "url: '/plugins/mahout',\n"
                + "templateUrl: 'plugins/mahout/partials/view.html',\n" +
				"data: {\n" +
				"bodyClass: '',\n" +
				"layout: 'default'\n" +
				"},\n" +
				"resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n" + "{\n"
                + "name: 'subutai.plugins.mahout',\n" + "files: [\n"
                + "'plugins/mahout/mahout.js',\n" + "'plugins/mahout/controller.js',\n"
                + "'plugins/mahout/service.js',\n" + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
                + "]);\n" + "}]\n" + "}\n" + "})";
	}
}
