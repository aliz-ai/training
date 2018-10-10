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
    
    public static void main(String[] args) {
        final IngestionTimeBasedPartitioningSamplePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(IngestionTimeBasedPartitioningSamplePipelineOptions.class);
        options.setTempLocation("gs://aliz-public-dataflow-templocation/temp/");
        options.setRunner(DataflowRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        
        TableSchema schema = new TableSchema();
        
        schema.setFields(Lists.newArrayList(new TableFieldSchema().setName("date").setType("DATE"),
                                            new TableFieldSchema().setName("year").setType("INTEGER"),
                                            new TableFieldSchema().setName("month").setType("INTEGER"),
                                            new TableFieldSchema().setName("day").setType("INTEGER"),
                                            new TableFieldSchema().setName("wikimedia_project").setType("STRING"),
                                            new TableFieldSchema().setName("language").setType("STRING"),
                                            new TableFieldSchema().setName("title").setType("STRING"),
                                            new TableFieldSchema().setName("views").setType("INTEGER")));
        
        String dataset = options.getDataset();
        String project = options.getProject();
        PCollection<TableRow> inputRows = pipeline.apply(BigQueryIO.readTableRows().from(new TableReference().setProjectId(project).setDatasetId(dataset).setTableId("Wiki10B")));
        
        inputRows.apply(BigQueryIO.writeTableRows()
                                 .withSchema(schema)
                                 .to(value -> {
                                     String date = (String) value.getValue().get("date");
                                     String dateString = date.replaceAll("-", "");
                                     String wikimediaProject = String.valueOf(value.getValue().get("wikimedia_project"));
                                     String language = String.valueOf(value.getValue().get("language"));
            
                                     return new TableDestination(new TableReference().setProjectId(project)
                                                                                     .setDatasetId(dataset)
                                                                                     .setTableId(String.format("Wiki10B_sharded_%s_%s_%s", dateString, language, wikimediaProject)), null);
            
                                 }));
    
        inputRows.apply(BigQueryIO.writeTableRows()
                                 .withSchema(schema)
                                 .to(value -> {
                                     String date = (String) value.getValue().get("date");
                                     String dateString = date.replaceAll("-", "");
                                     
        
                                     return new TableDestination(new TableReference().setProjectId(project)
                                                                                     .setDatasetId(dataset)
                                                                                     .setTableId(String.format("Wiki10B_ingestion_partitioned$%s", dateString)), null);
        
                                 }));
        
        pipeline.run();
    }
    
    public interface IngestionTimeBasedPartitioningSamplePipelineOptions extends DataflowPipelineOptions {
        
        String getDataset();
        
        void setDataset(String dataset);
        
    }
}
