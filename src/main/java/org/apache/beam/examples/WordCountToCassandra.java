package org.apache.beam.examples;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.concurrent.TimeUnit;


public class WordCountToCassandra {
    private static final String ASTRA_DB_ID = "<ChangeMe>";
    private static final String ASTRA_DB_REGION = "<ChangeMe>";
    private static final String ASTRA_DB_KEYSPACE = "test";
    private static final String ASTRA_TOKEN = "<ChangeMe>";
    static void runWordCount(WordCountToCassandra.WordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new WordCountToCassandra.CountWords())

                .apply("Write to gRPC AstraDB API", ParDo.of(new DoFn<KV<String, Long>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Long> record = c.element();

                        // Create Grpc Channel
                        ManagedChannel channel = ManagedChannelBuilder
                                .forAddress(ASTRA_DB_ID + "-" + ASTRA_DB_REGION + ".apps.astra.datastax.com", 443)
                                .useTransportSecurity()
                                .build();

                        // blocking stub version
                        StargateGrpc.StargateBlockingStub blockingStub =
                                StargateGrpc.newBlockingStub(channel)
                                        .withDeadlineAfter(10, TimeUnit.SECONDS)
                                        .withCallCredentials(new StargateBearerToken(ASTRA_TOKEN));

                        // INSERT
                        QueryOuterClass.Response queryString = blockingStub.executeQuery(
                                QueryOuterClass.Query.newBuilder()
                                        .setCql(String.format(
                                                "INSERT INTO %s.word (word, count) " +
                                                "VALUES ('%s', %d);", ASTRA_DB_KEYSPACE, record.getKey(), record.getValue()))
                                        .build());
                    }
                }));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        WordCountToCassandra.WordCountOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountToCassandra.WordCountOptions.class);

        runWordCount(options);
    }

    /**
     * Options supported by {@link WordCount}.
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface WordCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);
    }

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(WordCount.ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(WordCount.ExtractWordsFn.class, "lineLenDistro");
    }


    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new WordCount.ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }
}
