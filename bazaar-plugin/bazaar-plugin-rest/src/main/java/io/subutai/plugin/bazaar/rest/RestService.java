package io.subutai.plugin.bazaar.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public interface RestService
{
	@GET
	@Path( "products" )
	@Produces( { MediaType.APPLICATION_JSON } )
	public Response listProducts();
}