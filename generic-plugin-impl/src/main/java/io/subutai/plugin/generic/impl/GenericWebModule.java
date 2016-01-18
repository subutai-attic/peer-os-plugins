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
		return ".state('generic', {\n" +
				"url: '/plugins/generic',\n" +
				"templateUrl: 'plugins/generic/partials/view.html',\n" +
				"data: {\n" +
				"bodyClass: '',\n" +
				"layout: 'default'\n" +
				"},\n" +
				"resolve: {\n" +
				"loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"return $ocLazyLoad.load([\n" +
				"{\n" +
				"name: 'vtortola.ng-terminal'\n" +
				"},\n" +
				"{\n" +
				"name: 'subutai.plugins.generic',\n" +
				"files: [\n" +
				"'plugins/generic/generic.js',\n" +
				"'plugins/generic/controller.js',\n" +
				"'plugins/generic/service.js',\n" +
				"'subutai-app/environment/service.js'\n" +
				"]\n" +
				"}\n" +
				"]);\n" +
				"}]\n" +
				"}\n" +
				"})";
	}
}
