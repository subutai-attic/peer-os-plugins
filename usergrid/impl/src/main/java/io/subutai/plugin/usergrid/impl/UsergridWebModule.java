package io.subutai.plugin.usergrid.impl;

import io.subutai.webui.api.WebuiModule;

public class UsergridWebModule implements WebuiModule
{
	public static String NAME = "Usergrid";
	public static String IMG = "plugins/usergrid/usergrid.png";

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
		return ".state('usergrid', {\n" +
				"            url: '/plugins/usergrid',\n" +
				"            templateUrl: 'plugins/usergrid/partials/view.html',\n" +
				"            data: {\n" +
				"                bodyClass: '',\n" +
				"                layout: 'default'\n" +
				"            },\n" +
				"            resolve: {\n" +
				"                loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
				"                    return $ocLazyLoad.load([                          {\n" +
				"                        name: 'subutai.plugins.usergrid',\n" +
				"                        files: [\n" +
				"                            'plugins/usergrid/usergird.js',\n" +
				"                            'plugins/usergrid/controller.js',\n" +
				"                            'plugins/usergrid/service.js',\n" +
				"                            'subutai-app/environment/service.js'\n" +
				"                    ]}]);\n" +
				"            }]}})";
	}
}
