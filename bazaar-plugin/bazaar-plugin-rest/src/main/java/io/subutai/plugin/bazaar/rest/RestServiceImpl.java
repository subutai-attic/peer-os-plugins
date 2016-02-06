package io.subutai.plugin.bazaar.rest;

import io.subutai.plugin.bazaar.api.Bazaar;

import javax.ws.rs.core.Response;

public class RestServiceImpl implements RestService
{
	private Bazaar bazaar;

	@Override
	public Response listProducts ()
	{
		return Response.status( Response.Status.OK ).entity( bazaar.getProducts () ).build();
	}

	public void setBazaar (final Bazaar bazaar)
	{
		this.bazaar = bazaar;
	}
}
