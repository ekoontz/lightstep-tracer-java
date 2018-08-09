package com.sift.core.tracing;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.lightstep.tracer.jre.JRETracer;
import com.lightstep.tracer.shared.Options;
import com.lightstep.tracer.shared.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Span;
import org.apache.htrace.SpanReceiver;
import org.apache.htrace.TimelineAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LightstepSpanReceiver implements SpanReceiver {
    private static final Logger logger = LoggerFactory.getLogger(LightstepSpanReceiver.class);
    private final HTraceConfiguration conf;
    private static Object lock = new Object();
    private Boolean tracerInitialized = false;


    public LightstepSpanReceiver(HTraceConfiguration conf) {
        this.conf = conf;
    }

    /**
     * in htrace, values are untyped bytes. we attempt to parse these bytes into something
     * meaningful (first long, then double, bool, string, and finally bytes if nothing else works).
     */
    private static void setTag(io.opentracing.Span otSpan, String key, String value) {
        try {
            otSpan.setTag(key, Long.parseLong(value));
            return;
        } catch (NumberFormatException e) {
            // not a long
        }

        try {
            otSpan.setTag(key, Double.parseDouble(value));
            return;
        } catch (NumberFormatException e) {
            // not a double
        }

        if (value.equalsIgnoreCase("true")) {
            otSpan.setTag(key, true);
            return;
        }

        if (value.equalsIgnoreCase("false")) {
            otSpan.setTag(key, false);
            return;
        }

        otSpan.setTag(key, value);
    }

    @Override
    public void receiveSpan(Span span) {
        logger.info("Received htrace span: ", span.toJson());
        synchronized (lock) {
            if (!tracerInitialized) {
                logger.info("Initializing lightstep tracer.");
                this.initializeTracer(conf);
                tracerInitialized = true;
                logger.info("Initialized lightstep tracer.");
            }
        }
        String operationName = span.getDescription();

        com.lightstep.tracer.shared.SpanBuilder spanBuilder =
                (com.lightstep.tracer.shared.SpanBuilder) GlobalTracer.get().buildSpan(operationName)
                        // lightstep uses microseconds, while htrace uses milliseconds
                        .withStartTimestamp(span.getStartTimeMillis() * 1000);

        spanBuilder.withTraceIdAndSpanId(span.getTraceId(), span.getSpanId());

        if (span.getParents().length > 0) {
            spanBuilder.asChildOf(new SpanContext(span.getTraceId(), span.getParents()[0]));
        }

        io.opentracing.Span otSpan = spanBuilder.start();

        // handle key value annotations
        Map<String, String> kvAnnotations = span.getKVAnnotations();
        for (Map.Entry<String, String> kv : kvAnnotations.entrySet()) {
            setTag(otSpan, kv.getKey(), kv.getValue());
        }
        // handle timeline annotations
        List<TimelineAnnotation> timelineAnnotations = span.getTimelineAnnotations();
        for (TimelineAnnotation annotation : timelineAnnotations) {
            long time = annotation.getTime() * 1000;
            String message = annotation.getMessage();
            otSpan.log(time, message);
        }
        otSpan.finish(span.getStopTimeMillis() * 1000);
    }

    @Override
    public void close() throws IOException {
        // no close method on Tracer
    }

    /**
     * Initializes a Lighstep tracer to persist converted open trace spans into lightstep.
     * HBase configurations should be setup such that these fields are provided. Their naming
     * should have a prefix of the form, "hbase.htrace". For example, to specify the environment,
     * a config with the name "hbase.htrace.environment" should be set to e.g., "prod".
     *
     * An example configuration would look as follows:
     *
     * <property>
     *     <name>hbase.trace.spanreceiver.classes</name>
     *     <value>com.sift.core.tracing.LightstepSpanReceiver</value>
     * </property>
     * <property>
     *     <name>hbase.htrace.environment</name>
     *     <value>prod</value>
     * </property>
     *
     */
    private static void initializeTracer(HTraceConfiguration conf) {
        try {
            logger.info("initializeTracer(): starting..");
            Options options = new Options.OptionsBuilder()
                    .withAccessToken("your-access-token")
                    .withCollectorHost("your-lightstep-collector-hostname")
                    .withCollectorPort(5150)
                    .withCollectorProtocol("http")
                    .withComponentName("HBase tracer")
                    .withVerbosity(4)
                    .build();
            logger.info("initializeTracer(): created options object.");
            logger.info("starting call to JRETracer..");
            final Tracer tracer = new JRETracer(options);
            logger.info("initializeTracer(): created tracer object.");
            GlobalTracer.register(tracer);
            logger.info("initializeTracer(): registered tracer object as the global tracer.");
        } catch (MalformedURLException e) {
            System.out.println("failed to configure lightstep: " + e);
        }
    }
}
