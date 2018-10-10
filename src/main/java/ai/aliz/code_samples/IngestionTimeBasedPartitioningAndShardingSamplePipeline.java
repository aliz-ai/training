package ai.aliz.code_samples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class IngestionTimeBasedPartitioningAndShardingSamplePipeline {
    
    public static final String DATE = "date";
    public static final String WIKIMEDIA_PROJECT = "wikimedia_project";
    public static final String LANGUAGE = "language";
    public static final String WIKI_10B = "Wiki10B_";
    
    public static void main(String[] args) {
        final IngestionTimeBasedPartitioningSamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IngestionTimeBasedPartitioningSamplePipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        
        TableSchema schema = new TableSchema();
        
        schema.setFields(Lists.newArrayList(new TableFieldSchema().setName(DATE).setType("DATE"),
                                            new TableFieldSchema().setName("year").setType("INTEGER"),
                                            new TableFieldSchema().setName("month").setType("INTEGER"),
                                            new TableFieldSchema().setName("day").setType("INTEGER"),
                                            new TableFieldSchema().setName(WIKIMEDIA_PROJECT).setType("STRING"),
                                            new TableFieldSchema().setName(LANGUAGE).setType("STRING"),
                                            new TableFieldSchema().setName("title").setType("STRING"),
                                            new TableFieldSchema().setName("views").setType("INTEGER")));
        
        String dataset = options.getDataset();
        String project = options.getProject();
        PCollection<TableRow> inputRows = pipeline.apply(BigQueryIO.readTableRows().from(new TableReference().setProjectId(project).setDatasetId(dataset).setTableId("Wiki10B")));
        
        inputRows.apply(BigQueryIO.writeTableRows()
                                 .withSchema(schema)
                                 .to(value -> {
                                     String dateString = getDateString(value.getValue());
                                     String wikimediaProject = String.valueOf(value.getValue().get(WIKIMEDIA_PROJECT));
                                     String language = String.valueOf(value.getValue().get(LANGUAGE));
            
                                     return new TableDestination(new TableReference().setProjectId(project)
                                                                                     .setDatasetId(dataset)
                                                                                     .setTableId(String.format(WIKI_10B + "sharded_%s_%s_%s", dateString, language, wikimediaProject)), null);
            
                                 }));
    
        inputRows.apply(BigQueryIO.writeTableRows()
                                 .withSchema(schema)
                                 .to(value -> {
                                     String dateString = getDateString(value.getValue());
        
                                     return new TableDestination(new TableReference().setProjectId(project)
                                                                                     .setDatasetId(dataset)
                                                                                     .setTableId(String.format(WIKI_10B + "ingestion_partitioned$%s", dateString)), null);
        
                                 }));
        
        pipeline.run();
    }
    
    private static String getDateString(TableRow tableRow) {
        String date = (String) tableRow.get(DATE);
        return date.replaceAll("-", "");
    }
    
    public interface IngestionTimeBasedPartitioningSamplePipelineOptions extends DataflowPipelineOptions {
        
        String getDataset();
        
        void setDataset(String dataset);
        
    }
}
