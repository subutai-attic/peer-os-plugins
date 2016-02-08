package io.subutai.plugin.bazaar.impl;


import io.subutai.plugin.bazaar.api.Bazaar;
import io.subutai.plugin.hub.api.HubPluginException;
import io.subutai.plugin.hub.api.Integration;

public class BazaarImpl implements Bazaar
{

	private Integration integration;

	public BazaarImpl (final Integration integration)
	{
		this.integration = integration;
		try
		{
			this.integration.registerPeer ("hub.subut.ai");
		}
		catch (HubPluginException e)
		{
			e.printStackTrace ();
		}
	}


	@Override
	public String getProducts()
	{
		try
		{
			String result = integration.sendRequestToHub ("/rest/v1/marketplace/products");
			return result;
		}
		catch (HubPluginException e)
		{
			e.printStackTrace ();
		}
		return "";
	}
}
