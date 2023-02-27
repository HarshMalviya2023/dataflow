package com.impetus.dataflow;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

/**
 * This program reads data from BQ table apply DLP on the data Put the data into
 * a stage BQ table and then finally insert all data in final output bq table
 */
public class IngestionPipeline {
	
	/**
	 * slf4j log
	 */
	private static final Logger LOG = LoggerFactory.getLogger(IngestionPipeline.class);

	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

	/**
	 * The pipeline options/inputs taken from user
	 *
	 */
	public interface PipelineOptions extends DataflowPipelineOptions {

		@Description("Query to get the data from BigQuery " + "e.g. select field1, field2 from projectId.dataset.table")
		ValueProvider<String> getQuery();
		void setQuery(ValueProvider<String> value);

		@Description("DLP Deidentify Template to be used for API request "
				+ "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
		@Required
		ValueProvider<String> getDeidentifyTemplateName();
		void setDeidentifyTemplateName(ValueProvider<String> value);

		@Description("DLP Inspect Template to be used for API request "
				+ "(e.g.projects/{project_id}/inspectTemplates/{inspectTemplateId}")
		ValueProvider<String> getInspectTemplateName();
		void setInspectTemplateName(ValueProvider<String> value);

		@Description("Big Query Output table")
		ValueProvider<String> getOutputTableSpec();
		void setOutputTableSpec(ValueProvider<String> value);

		@Description("ProjectId")
		ValueProvider<String> getProjectId();

		void setProjectId(ValueProvider<String> value);
	}

	/**
	 *  Transforms BigQuery TableRow to DLP Table
	 *  extends DoFn 
	 */
	static class TransformBQData extends DoFn<TableRow, Table> {

		private static final long serialVersionUID = 1L;
		private PCollectionView<List<List<String>>> headers;

		public TransformBQData(PCollectionView<List<List<String>>> headers) {
			this.headers = headers;
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow tableRow = c.element();
			List<Table.Row> rows = new ArrayList<>();
			List<String> listOfHeaders = getHeaders(c.sideInput(headers));
			List<FieldId> dlpTableHeaders = listOfHeaders.stream()
					.map(header -> FieldId.newBuilder().setName(header).build()).collect(Collectors.toList());

			Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();

			for (String header : listOfHeaders) {
				String value = tableRow.get(header).toString();
				if (value != null) {
					tableRowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
				} else {
					tableRowBuilder.addValues(Value.newBuilder().setStringValue("").build());
				}
			}
			rows.add(tableRowBuilder.build());
			c.output(Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build());
		}

		private List<String> getHeaders(List<List<String>> sideInput) {
			return sideInput.stream().findFirst().get();
		}
	}

	/**
	 * 	Transforms DLP Table to BigQuery TableRow  
	 *	extends DoFn
	 */
	public static class TableRowProcessorDoFn extends DoFn<Table, TableRow> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			Table tokenizedData = c.element();
			List<String> headers = tokenizedData.getHeadersList().stream().map(fid -> fid.getName())
					.collect(Collectors.toList());
			List<Table.Row> outputRows = tokenizedData.getRowsList();
			if (outputRows.size() > 0) {
				for (Table.Row outputRow : outputRows) {
					if (outputRow.getValuesCount() != headers.size()) {
						throw new IllegalArgumentException(
								"CSV file's header count must exactly match with data element count");
					}
					LOG.info("outputRow.getValuesCount()" + outputRow.getValuesCount());
					c.output(createBqRow(outputRow, headers.toArray(new String[headers.size()])));
				}
			}
		}

		private static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {

			TableRow bqRow = new TableRow();
			AtomicInteger headerIndex = new AtomicInteger(0);
			tokenizedValue.getValuesList().forEach(value -> {
				String checkedHeaderName = checkHeaderName(headers[headerIndex.getAndIncrement()].toString());
				bqRow.set(checkedHeaderName, value.getStringValue());
			});
			return bqRow;
		}

		private static String checkHeaderName(String name) {
			/**
			 * some checks to make sure BQ column names don't fail e.g. special characters
			 */
			String checkedHeader = name.replaceAll("\\s", "_");
			checkedHeader = checkedHeader.replaceAll("'", "");
			checkedHeader = checkedHeader.replaceAll("/", "");
			return checkedHeader;
		}
	}

	/**
	 * Used to get schema for output bigquery table
	 * extends DoFn
	 */
	public static class GetBqSchema extends DoFn<TableRow, KV<String, String>> {
		private ValueProvider<String> tableSpec;

		public GetBqSchema(ValueProvider<String> tableSpec) {
			this.tableSpec = tableSpec;
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			if (!BqObject.getInstance().isRunOnce()) {
				ArrayNode schemaArrayNode = new ObjectMapper().createArrayNode();
				for (Entry<String, Object> i : c.element().entrySet()) {
					ObjectNode scccc = new ObjectMapper().createObjectNode();
					String key = i.getKey();
					scccc.put("name", key);
					scccc.put("type", "STRING");
					scccc.put("mode", "NULLABLE");
					schemaArrayNode.add(scccc);
				}
				BqObject.getInstance().setRunOnce(true);
				ObjectNode node = new ObjectMapper().createObjectNode();
				node.set("fields", schemaArrayNode);
				c.output(KV.of(tableSpec.get(), node.toString()));
			}
		}
	}
	
	/**
	 * Class to apply dlp to bigQuery data 
	 * extends DoFn
	 */
	static class DataTokenization extends DoFn<Table, Table> {
		private static final long serialVersionUID = 1L;
		private ValueProvider<String> dlpProjectId;
		private DlpServiceClient dlpServiceClient;
		private ValueProvider<String> deIdentifyTemplateName;
		private ValueProvider<String> inspectTemplateName;
		private ValueProvider<String> headers;
		private boolean inspectTemplateExist;
		private Builder requestBuilder;
		private final Distribution numberOfRowsTokenized = Metrics.distribution(DataTokenization.class,
				"numberOfRowsTokenizedDistro");
		private final Distribution numberOfBytesTokenized = Metrics.distribution(DataTokenization.class,
				"numberOfBytesTokenizedDistro");

		public DataTokenization(ValueProvider<String> dlpProjectId, ValueProvider<String> deIdentifyTemplateName,
				ValueProvider<String> inspectTemplateName) {
			this.dlpProjectId = dlpProjectId;
			this.dlpServiceClient = null;
			this.deIdentifyTemplateName = deIdentifyTemplateName;
			this.inspectTemplateName = inspectTemplateName;
			this.inspectTemplateExist = false;
		}

		@Setup
		public void setup() {
			if (this.inspectTemplateName.isAccessible()) {
				if (this.inspectTemplateName.get() != null) {
					this.inspectTemplateExist = true;
				}
			}
			LOG.info("deIdentifyTemplateName:" + deIdentifyTemplateName.get());
			if (this.deIdentifyTemplateName.isAccessible()) {
				if (this.deIdentifyTemplateName.get() != null) {
					this.requestBuilder = DeidentifyContentRequest.newBuilder()
							.setParent(ProjectName.of(this.dlpProjectId.get()).toString())
							.setDeidentifyTemplateName(this.deIdentifyTemplateName.get());
					if (this.inspectTemplateExist) {
						this.requestBuilder.setInspectTemplateName(this.inspectTemplateName.get());
					}
				}
			}
		}

		@StartBundle
		public void startBundle() throws SQLException {

			try {
				this.dlpServiceClient = DlpServiceClient.create();

			} catch (IOException e) {
				LOG.error("Failed to create DLP Service Client", e.getMessage());
				throw new RuntimeException(e);
			}
		}

		@FinishBundle
		public void finishBundle() throws Exception {
			if (this.dlpServiceClient != null) {
				this.dlpServiceClient.close();
			}
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			Table nonEncryptedData = c.element();
			ContentItem tableItem = ContentItem.newBuilder().setTable(nonEncryptedData).build();
			this.requestBuilder.setItem(tableItem);
			DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(this.requestBuilder.build());

			Table tokenizedData = response.getItem().getTable();
			numberOfRowsTokenized.update(tokenizedData.getRowsList().size());
			numberOfBytesTokenized.update(tokenizedData.toByteArray().length);
			c.output(tokenizedData);
		}
	}

	public static void main(String[] args) {
		// define pipeline options

		List<String> headers = new ArrayList<String>();

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

		// create Pipeline with options
		Pipeline p = Pipeline.create(options);

		// get data from BQ1 as TableRow
		PCollection<TableRow> bigQueryOutput = p.apply("Get-BigQuery-Data",
				BigQueryIO.readTableRows().fromQuery(options.getQuery()).withoutValidation().usingStandardSql());

		PCollectionView<List<List<String>>> fileHeaders = bigQueryOutput
				.apply("Get-File-Headers", ParDo.of(new DoFn<TableRow, List<String>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						if (headers.isEmpty()) {
							for (Entry<String, Object> i : c.element().entrySet()) {
								String key = i.getKey();
								headers.add(key);
							}
						}
						c.output(headers);
					}
				})).apply("View-As-List", View.asList());

		PCollectionView<Map<String, String>> bqSchema = bigQueryOutput
				.apply("Get-Output-BigQueryTable-Schema", ParDo.of(new GetBqSchema(options.getOutputTableSpec())))
				.apply(View.asMap());

		PCollection<Table> tableRowData = bigQueryOutput.apply("TransformBQData-to-DLP.Table",
				ParDo.of(new TransformBQData(fileHeaders)).withSideInputs(fileHeaders));

		PCollection<TableRow> dlpTokenizedData = tableRowData
				.apply("DLP-Tokenization",
						ParDo.of(new DataTokenization(options.getProjectId(), options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))

				.apply("Transform Tokenized Data Back to BQData", ParDo.of(new TableRowProcessorDoFn()));

                 dlpTokenizedData.apply("Write-dlp-data-to-bigQuery", BigQueryIO.writeTableRows()
				// .to(stgTableRef)
				.to(options.getOutputTableSpec()).withSchemaFromView(bqSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run();
	}
}
