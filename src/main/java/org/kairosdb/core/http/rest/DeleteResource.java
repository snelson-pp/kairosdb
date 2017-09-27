package org.kairosdb.core.http.rest;

import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.util.MemoryMonitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

import static org.kairosdb.core.http.rest.DefaultResource.setHeaders;
import static org.kairosdb.core.http.rest.DefaultResource.getCorsPreflightResponseBuilder;



@Path("/api/v1")
public class DeleteResource
{
	public static final Logger logger = LoggerFactory.getLogger(DeleteResource.class);

    private final KairosDatastore datastore;
    private final QueryParser queryParser;

	@Inject
	public DeleteResource(KairosDatastore datastore, QueryParser queryParser)
	{
		this.datastore = checkNotNull(datastore);
		this.queryParser = checkNotNull(queryParser);
    }

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/delete")
	public Response corsPreflightDelete(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										@HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/delete")
	public Response delete(String json) throws Exception
	{
		checkNotNull(json);
		logger.debug(json);

		try
		{
			List<QueryMetric> queries = queryParser.parseQueryMetric(json).getQueryMetrics();

			for (QueryMetric query : queries)
			{
				datastore.delete(query);
			}

			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		}
		catch (JsonSyntaxException |QueryException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (BeanValidationException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addErrors(e.getErrorMessages()).build();
		}
		catch (MemoryMonitorException e)
		{
			logger.error("Query failed.", e);
			System.gc();
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
		catch (Exception e)
		{
			logger.error("Delete failed.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

		}catch (OutOfMemoryError e)
		{
			logger.error("Out of memory error.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metric/{metricName}")
	public Response corsPreflightMetricDelete(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
											  @HeaderParam("Access-Control-Request-Method") String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metric/{metricName}")
	public Response metricDelete(@PathParam("metricName") String metricName) throws Exception
	{
		try
		{
			QueryMetric query = new QueryMetric(Long.MIN_VALUE, Long.MAX_VALUE, 0, metricName);
			datastore.delete(query);


			return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
		} catch (Exception e)
		{
			logger.error("Delete failed.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

}