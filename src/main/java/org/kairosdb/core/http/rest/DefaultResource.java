package org.kairosdb.core.http.rest;

import com.google.inject.Inject;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.JsonFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

import static javax.ws.rs.core.Response.ResponseBuilder;

@Path("/api/v1")
public class DefaultResource
{
	public static final Logger logger = LoggerFactory.getLogger(DefaultResource.class);
	private final Map<String, DataFormatter> formatters = new HashMap<>();

	@Inject
	public DefaultResource()
	{
		formatters.put("json", new JsonFormatter());

	}

	public static ResponseBuilder setHeaders(ResponseBuilder responseBuilder)
	{
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Pragma", "no-cache");
		responseBuilder.header("Cache-Control", "no-cache");
		responseBuilder.header("Expires", 0);

		return (responseBuilder);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/version")
	public Response corsPreflightVersion(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										 @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/version")
	public Response getVersion()
	{
		Package thisPackage = getClass().getPackage();
		String versionString = thisPackage.getImplementationTitle() + " " + thisPackage.getImplementationVersion();
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity("{\"version\": \"" + versionString + "\"}");
		setHeaders(responseBuilder);
		return responseBuilder.build();
	}

	public static ResponseBuilder getCorsPreflightResponseBuilder(final String requestHeaders,
																  final String requestMethod)
	{
		ResponseBuilder responseBuilder = Response.status(Response.Status.OK);
		responseBuilder.header("Access-Control-Allow-Origin", "*");
		responseBuilder.header("Access-Control-Allow-Headers", requestHeaders);
		responseBuilder.header("Access-Control-Max-Age", "86400"); // Cache for one day
		if (requestMethod != null)
		{
			responseBuilder.header("Access-Control-Allow_Method", requestMethod);
		}

		return responseBuilder;
	}
}