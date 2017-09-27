package org.kairosdb.core.http.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.MalformedJsonException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.formatter.DataFormatter;
import org.kairosdb.core.formatter.JsonFormatter;
import org.kairosdb.core.http.rest.json.DataPointsParser;
import org.kairosdb.core.http.rest.json.ErrorResponse;
import org.kairosdb.core.http.rest.json.JsonResponseBuilder;
import org.kairosdb.core.http.rest.json.ValidationErrors;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.EventBusWithFilters;
import org.kairosdb.util.SimpleStats;
import org.kairosdb.util.SimpleStatsReporter;
import org.kairosdb.util.StatsMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.ResponseBuilder;

import static org.kairosdb.core.http.rest.DefaultResource.setHeaders;
import static org.kairosdb.core.http.rest.DefaultResource.getCorsPreflightResponseBuilder;


@Path("/api/v1/datapoints")
public class IngestResource implements KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(IngestResource.class);
	public static final String REQUEST_TIME = "kairosdb.http.request_time";
	public static final String INGEST_COUNT = "kairosdb.http.ingest_count";
	public static final String INGEST_TIME = "kairosdb.http.ingest_time";

	private final KairosDatastore datastore;
	private final EventBusWithFilters m_eventBus;
	private final Map<String, DataFormatter> formatters = new HashMap<>();

	//Used for parsing incoming metrics
	private final Gson gson;

	//These two are used to track rate of ingestion
	private final AtomicInteger m_ingestedDataPoints = new AtomicInteger();
	private final AtomicInteger m_ingestTime = new AtomicInteger();

	private final StatsMap m_statsMap = new StatsMap();private final KairosDataPointFactory m_kairosDataPointFactory;

	@Inject
	private LongDataPointFactory m_longDataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	private StringDataPointFactory m_stringDataPointFactory = new StringDataPointFactory();

	@Inject
	@Named("HOSTNAME")
	private String hostName = "localhost";

	@Inject
	private SimpleStatsReporter m_simpleStatsReporter = new SimpleStatsReporter();

	@Inject
	public IngestResource(KairosDatastore datastore, KairosDataPointFactory dataPointFactory, EventBusWithFilters eventBus)
	{
		this.datastore = checkNotNull(datastore);
		m_kairosDataPointFactory = dataPointFactory;
		m_eventBus = checkNotNull(eventBus);
		formatters.put("json", new JsonFormatter());

		GsonBuilder builder = new GsonBuilder();
		gson = builder.disableHtmlEscaping().create();
	}

	@OPTIONS
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response corsPreflightDataPoints(@HeaderParam("Access-Control-Request-Headers") String requestHeaders,
											@HeaderParam("Access-Control-Request-Method") String requestMethod)
	{
		ResponseBuilder responseBuilder = getCorsPreflightResponseBuilder(requestHeaders, requestMethod);
		return (responseBuilder.build());
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Consumes("application/gzip")
	@Path("/datapoints")
	public Response addGzip(InputStream gzip)
	{
		GZIPInputStream gzipInputStream;
		try
		{
			gzipInputStream = new GZIPInputStream(gzip);
		} catch (IOException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		return (add(gzipInputStream));
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/datapoints")
	public Response add(InputStream json)
	{
		try
		{
			DataPointsParser parser = new DataPointsParser(m_eventBus, new InputStreamReader(json, "UTF-8"),
					gson, m_kairosDataPointFactory);
			ValidationErrors validationErrors = parser.parse();

			m_ingestedDataPoints.addAndGet(parser.getDataPointCount());
			m_ingestTime.addAndGet(parser.getIngestTime());

			if (!validationErrors.hasErrors())
				return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
			else
			{
				JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
				for (String errorMessage : validationErrors.getErrors())
				{
					builder.addError(errorMessage);
				}
				return builder.build();
			}
		}
		catch (JsonIOException |MalformedJsonException | JsonSyntaxException e)
		{
			JsonResponseBuilder builder = new JsonResponseBuilder(Response.Status.BAD_REQUEST);
			return builder.addError(e.getMessage()).build();
		}
		catch (Exception e)
		{
			logger.error("Failed to add metric.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();

		}catch (OutOfMemoryError e)
		{
			logger.error("Out of memory error.", e);
			return setHeaders(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new ErrorResponse(e.getMessage()))).build();
		}
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		int time = m_ingestTime.getAndSet(0);
		int count = m_ingestedDataPoints.getAndSet(0);
		List<DataPointSet> ret = new ArrayList<>();

		if (count != 0)
		{

			DataPointSet dpsCount = new DataPointSet(INGEST_COUNT);
			DataPointSet dpsTime = new DataPointSet(INGEST_TIME);

			dpsCount.addTag("host", hostName);
			dpsTime.addTag("host", hostName);

			dpsCount.addDataPoint(m_longDataPointFactory.createDataPoint(now, count));
			dpsTime.addDataPoint(m_longDataPointFactory.createDataPoint(now, time));

			ret.add(dpsCount);
			ret.add(dpsTime);}

		Map<String, SimpleStats> statsMap = m_statsMap.getStatsMap();

		for (Map.Entry<String, SimpleStats> entry : statsMap.entrySet())
		{
			String metric = entry.getKey();
			SimpleStats.Data stats = entry.getValue().getAndClear();

			m_simpleStatsReporter.reportStats(stats, now, metric, ret);
		}

		return ret;
	}
}