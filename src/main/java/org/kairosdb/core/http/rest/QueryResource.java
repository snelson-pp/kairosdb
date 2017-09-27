package org.kairosdb.core.http.rest;

import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryPlugin;
import org.kairosdb.core.datastore.QueryPostProcessingPlugin;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.formatter.JsonResponse;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.Query;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.reporting.ThreadReporter;
import org.kairosdb.eventbus.EventBusWithFilters;
import org.kairosdb.util.MemoryMonitorException;
import org.kairosdb.util.StatsMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

import static org.kairosdb.core.http.rest.DefaultResource.setHeaders;
import static org.kairosdb.core.http.rest.DefaultResource.getCorsPreflightResponseBuilder;


enum NameType
{
	METRIC_NAMES,
	TAG_KEYS,
	TAG_VALUES
}

//@Path("/api/v1")
public class QueryResource
{
	public static final Logger logger = LoggerFactory.getLogger(QueryResource.class);
	public static final String QUERY_TIME = "kairosdb.http.query_time";
	public static final String REQUEST_TIME = "kairosdb.http.request_time";

	public static final String QUERY_URL = "/datapoints/query";

	private final KairosDatastore datastore;
	private final EventBusWithFilters m_eventBus;
	private final Map<String, DataFormatter> formatters = new HashMap<>();
	private final QueryParser queryParser;

	private final StatsMap m_statsMap = new StatsMap();

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private StringDataPointFactory m_stringDataPointFactory = new StringDataPointFactory();

	@Inject
	private QueryPreProcessorContainer m_queryPreProcessor = new QueryPreProcessorContainer()
	{
		@Override
		public Query preProcess(Query query)
		{
			return query;
		}
	};

	@Inject(optional=true)
	@Named("kairosdb.queries.aggregate_stats")
	private boolean m_aggregatedQueryMetrics = false;

	@Inject(optional = true)
	@Named("kairosdb.log.queries.enable")
	private boolean m_logQueries = false;

	@Inject(optional = true)
	@Named("kairosdb.log.queries.ttl")
	private int m_logQueriesTtl = 86400;

	@Inject(optional = true)
	@Named("kairosdb.log.queries.greater_than")
	private int m_logQueriesLongerThan = 60;

	@Inject
	@Named("HOSTNAME")
	private String hostName = "localhost";

	@Inject
	public QueryResource(KairosDatastore datastore, QueryParser queryParser, EventBusWithFilters eventBus)
	{
		this.datastore = checkNotNull(datastore);
		this.queryParser = checkNotNull(queryParser);
		m_eventBus = checkNotNull(eventBus);
		formatters.put("json", new JsonFormatter());
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response corsPreflightMetricNames(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
											 @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/metricnames")
	public Response getMetricNames()
	{
		return executeNameQuery(NameType.METRIC_NAMES);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response corsPreflightTagNames(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										  @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagnames")
	public Response getTagNames()
	{
		return executeNameQuery(NameType.TAG_KEYS);
	}


	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagvalues")
	public Response corsPreflightTagValues(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										   @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/tagvalues")
	public Response getTagValues()
	{
		return executeNameQuery(NameType.TAG_VALUES);
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query/tags")
	public Response corsPreflightQueryTags(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
										   @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints/query/tags")
	public Response getMeta(String json)
	{
		checkNotNull(json);
		logger.debug(json);

		try
		{
			File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
			BufferedWriter writer = new BufferedWriter(new FileWriter(respFile));

			JsonResponse jsonResponse = new JsonResponse(writer);

			jsonResponse.begin();

			List<QueryMetric> queries = queryParser.parseQueryMetric(json).getQueryMetrics();

			for (QueryMetric query : queries)
			{
				List<DataPointGroup> result = datastore.queryTags(query);

				try
				{
					jsonResponse.formatQuery(result, false, -1);
				} finally
				{
					for (DataPointGroup dataPointGroup : result)
					{
						dataPointGroup.close();
					}
				}
			}

			jsonResponse.end();
			writer.flush();
			writer.close();

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new FileStreamingOutput(respFile));

			setHeaders(responseBuilder);
			return responseBuilder.build();
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
			logger.error("Query failed.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

		}catch (OutOfMemoryError e)
		{
			logger.error("Out of memory error.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	/**
	 * Information for this endpoint was taken from https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS.
	 * <p/>
	 * <p/>Response to a cors preflight request to access data.
	 */
	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response corsPreflightQuery(@HeaderParam("Access-Control-Request-Headers") final String requestHeaders,
									   @HeaderParam("Access-Control-Request-Method") final String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response getQuery(@QueryParam("query") String json, @Context HttpServletRequest request) throws Exception
	{
		return runQuery(json, request.getRemoteAddr());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path(QUERY_URL)
	public Response postQuery(String json, @Context HttpServletRequest request) throws Exception
	{
		return runQuery(json, request.getRemoteAddr());
	}


	public Response runQuery(String json, String remoteAddr) throws Exception
	{
		logger.debug(json);

		ThreadReporter.setReportTime(System.currentTimeMillis());
		ThreadReporter.addTag("host", hostName);

		try
		{
			if (json == null)
				throw new BeanValidationException(new QueryParser.SimpleConstraintViolation("query json", "must not be null or empty"), "");

			File respFile = File.createTempFile("kairos", ".json", new File(datastore.getCacheDir()));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(respFile), "UTF-8"));

			JsonResponse jsonResponse = new JsonResponse(writer);

			jsonResponse.begin();

			Query mainQuery = queryParser.parseQueryMetric(json);mainQuery = m_queryPreProcessor.preProcess(mainQuery);

			List<QueryMetric> queries = mainQuery.getQueryMetrics();

			int queryCount = 0;
			for (QueryMetric query : queries)
			{
				queryCount++;
				ThreadReporter.addTag("metric_name", query.getName());
				ThreadReporter.addTag("query_index", String.valueOf(queryCount));

				DatastoreQuery dq = datastore.createQuery(query);
				long startQuery = System.currentTimeMillis();

				try
				{
					List<DataPointGroup> results = dq.execute();
					jsonResponse.formatQuery(results, query.isExcludeTags(), dq.getSampleSize());

					ThreadReporter.addDataPoint(QUERY_TIME, System.currentTimeMillis() - startQuery);
				} finally
				{
					dq.close();
				}
			}

			jsonResponse.end();
			writer.flush();
			writer.close();

			ThreadReporter.clearTags();
			ThreadReporter.addTag("host", hostName);

			//write metrics for query logging
			long queryTime = System.currentTimeMillis() - ThreadReporter.getReportTime();
			if (m_logQueries && ((queryTime / 1000) >= m_logQueriesLongerThan))
			{
				ThreadReporter.addDataPoint("kairosdb.log.query.remote_address", remoteAddr, m_logQueriesTtl);
				ThreadReporter.addDataPoint("kairosdb.log.query.json", json, m_logQueriesTtl);
			}

			ThreadReporter.addTag("request", QUERY_URL);
			ThreadReporter.addDataPoint(REQUEST_TIME, queryTime);



			if (m_aggregatedQueryMetrics)
			{
				ThreadReporter.gatherData(m_statsMap);
			}
			else
			{ThreadReporter.submitData(m_longDataPointFactory,
					m_stringDataPointFactory, m_eventBus);
			}

			//System.out.println("About to process plugins");
			List<QueryPlugin> plugins = mainQuery.getPlugins();
			for (QueryPlugin plugin : plugins)
			{
				if (plugin instanceof QueryPostProcessingPlugin)
				{
					respFile = ((QueryPostProcessingPlugin)plugin).processQueryResults(respFile);
				}
			}

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new FileStreamingOutput(respFile));

			setHeaders(responseBuilder);
			return responseBuilder.build();
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
			Thread.sleep(1000);
			System.gc();
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
		catch (IOException e)
		{
			logger.error("Failed to open temp folder " + datastore.getCacheDir(), e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
		catch (Exception e)
		{
			logger.error("Query failed.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
		catch (OutOfMemoryError e)
		{
			logger.error("Out of memory error.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

		}finally
		{
			ThreadReporter.clear();
		}
	}

	private Response executeNameQuery(NameType type)
	{
		try
		{
			Iterable<String> values = null;
			switch (type)
			{
				case METRIC_NAMES:
					values = datastore.getMetricNames();
					break;
				case TAG_KEYS:
					values = datastore.getTagNames();
					break;
				case TAG_VALUES:
					values = datastore.getTagValues();
					break;
			}

			DataFormatter formatter = formatters.get("json");

			ResponseBuilder responseBuilder = Response.status(Response.Status.OK).entity(
					new ValuesStreamingOutput(formatter, values));
			setHeaders(responseBuilder);
			return responseBuilder.build();
		} catch (Exception e)
		{
			logger.error("Failed to get " + type, e);
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
					new ErrorResponse(e.getMessage())).build();
		}
	}

	public static class ValuesStreamingOutput implements StreamingOutput
	{
		private DataFormatter m_formatter;
		private Iterable<String> m_values;

		public ValuesStreamingOutput(DataFormatter formatter, Iterable<String> values)
		{
			m_formatter = formatter;
			m_values = values;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		public void write(OutputStream output) throws IOException, WebApplicationException
		{
			Writer writer = new OutputStreamWriter(output, "UTF-8");

			try
			{
				m_formatter.format(writer, m_values);
			} catch (FormatterException e)
			{
				logger.error("Description of what failed:", e);
			}

			writer.flush();
		}
	}

	public static class FileStreamingOutput implements StreamingOutput
	{
		private File m_responseFile;

		public FileStreamingOutput(File responseFile)
		{
			m_responseFile = responseFile;
		}

		@SuppressWarnings("ResultOfMethodCallIgnored")
		@Override
		public void write(OutputStream output) throws IOException, WebApplicationException
		{
			try
			{
				InputStream reader = new FileInputStream(m_responseFile);

				byte[] buffer = new byte[1024];
				int size;

				while ((size = reader.read(buffer)) != -1)
				{
					output.write(buffer, 0, size);
				}

				reader.close();
				output.flush();
			} finally
			{
				m_responseFile.delete();
			}
		}
	}

}