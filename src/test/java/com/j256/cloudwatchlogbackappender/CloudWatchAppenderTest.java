package com.j256.cloudwatchlogbackappender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import org.easymock.IAnswer;
import org.junit.Test;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.util.List;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class CloudWatchAppenderTest {

	@Test(timeout = 5000)
	public void testBasic() throws InterruptedException {
		CloudWatchAppender appender = new CloudWatchAppender();
		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setCloudWatchLogsClient(awsLogClient);

		appender.setMaxBatchSize(1);
		appender.setRegion("region");
		final String logGroup = "pfqoejpfqe";
		appender.setLogGroup(logGroup);
		final String logStream = "pffqjfqjpoqoejpfqe";
		appender.setLogStream(logStream);
		PatternLayout layout = new PatternLayout();
		layout.setContext(new LoggerContext());
		layout.setPattern("[%thread] %level %logger{20} - %msg%n%xThrowable");
		layout.start();
		appender.setLayout(layout);

		LoggingEvent event = new LoggingEvent();
		event.setTimeStamp(System.currentTimeMillis());
		String loggerName = "name";
		event.setLoggerName(loggerName);
		Level level = Level.DEBUG;
		event.setLevel(level);
		String message = "fjpewjfpewjfpewjfepowf";
		event.setMessage(message);

		String threadName = Thread.currentThread().getName();
		final String fullMessage = "[" + threadName + "] " + level + " " + loggerName + " - " + message + "\n";

		final PutLogEventsResponse result = PutLogEventsResponse.builder().build();

		result.nextSequenceToken();
		expect(awsLogClient.putLogEvents(isA(PutLogEventsRequest.class))).andAnswer(() -> {
            PutLogEventsRequest request = (PutLogEventsRequest) getCurrentArguments()[0];
            assertEquals(logGroup, request.logGroupName());
            assertEquals(logStream, request.logStreamName());
            List<InputLogEvent> events = request.logEvents();
            assertEquals(1, events.size());
            assertEquals(fullMessage, events.get(0).message());
            return result;
        }).times(2);
		awsLogClient.close();

		// =====================================

		replay(awsLogClient);
		appender.start();
		// for coverage
		appender.start();
		appender.append(event);
		Thread.sleep(10);
		appender.append(event);
		while (appender.getEventsWrittenCount() < 2) {
			Thread.sleep(10);
		}
		appender.stop();
		verify(awsLogClient);
	}

	@Test(timeout = 5000)
	public void testEmergencyAppender() throws InterruptedException {
		CloudWatchAppender appender = new CloudWatchAppender();
		CloudWatchLogsClient awsLogClient = createMock(CloudWatchLogsClient.class);
		appender.setCloudWatchLogsClient(awsLogClient);

		appender.setMaxBatchSize(1);
		appender.setRegion("region");
		final String logGroup = "pfqoejpfqe";
		appender.setLogGroup(logGroup);
		final String logStream = "pffqjfqjpoqoejpfqe";
		appender.setLogStream(logStream);
		PatternLayout layout = new PatternLayout();
		layout.setContext(new LoggerContext());
		layout.setPattern("[%thread] %level %logger{20} - %msg%n%xThrowable");
		layout.start();
		appender.setLayout(layout);

		LoggingEvent event = new LoggingEvent();
		event.setTimeStamp(System.currentTimeMillis());
		final String loggerName = "name";
		event.setLoggerName(loggerName);
		final Level level = Level.DEBUG;
		event.setLevel(level);
		String message = "fjpewjfpewjfpewjfepowf";
		event.setMessage(message);
		final String threadName = Thread.currentThread().getName();

		expect(awsLogClient.putLogEvents(isA(PutLogEventsRequest.class)))
				.andThrow(new RuntimeException("force emergency log")).anyTimes();
		awsLogClient.close();

		// =====================================

		@SuppressWarnings("unchecked")
		Appender<ILoggingEvent> emergencyAppender = (Appender<ILoggingEvent>) createMock(Appender.class);
		String emergencyAppenderName = "fjpeowjfwfewf";
		expect(emergencyAppender.getName()).andReturn(emergencyAppenderName);
		expect(emergencyAppender.isStarted()).andReturn(false);
		emergencyAppender.start();
		emergencyAppender.doAppend(isA(ILoggingEvent.class));
		expectLastCall().andAnswer((IAnswer<Void>) () -> {
            ILoggingEvent event1 = (ILoggingEvent) getCurrentArguments()[0];
            if (event1.getLevel() == level) {
                assertEquals(loggerName, event1.getLoggerName());
                assertEquals(threadName, event1.getThreadName());
            } else {
                assertEquals(Level.ERROR, event1.getLevel());
            }
            return null;
        }).times(2);
		emergencyAppender.stop();

		// =====================================

		replay(awsLogClient, emergencyAppender);
		assertNull(appender.getAppender(emergencyAppenderName));
		assertFalse(appender.isAttached(emergencyAppender));
		appender.addAppender(emergencyAppender);
		assertTrue(appender.isAttached(emergencyAppender));
		assertSame(emergencyAppender, appender.getAppender(emergencyAppenderName));
		assertNull(appender.getAppender(null));
		appender.start();
		// for coverage
		appender.start();
		appender.append(event);
		Thread.sleep(100);
		appender.detachAndStopAllAppenders();
		appender.stop();
		verify(awsLogClient, emergencyAppender);
	}

	@Test(timeout = 5000)
	public void testCoverage() {
		CloudWatchAppender appender = new CloudWatchAppender();
		assertFalse(appender.isWarningMessagePrinted());
		System.err.println("Expected warning on next line");
		appender.append(null);
		appender.append(null);
		assertTrue(appender.isWarningMessagePrinted());
		try {
			appender.start();
			fail("Should have thrown");
		} catch (IllegalStateException ise) {
			// expected
		}
		appender.setRegion("region");
		try {
			appender.start();
			fail("Should have thrown");
		} catch (IllegalStateException ise) {
			// expected
		}
		appender.setLogGroup("log-group");
		try {
			appender.start();
			fail("Should have thrown");
		} catch (IllegalStateException ise) {
			// expected
		}
		appender.setLogStream("log-group");
		try {
			appender.start();
			fail("Should have thrown");
		} catch (IllegalStateException ise) {
			// expected
		}
	}

}
