package com.j256.cloudwatchlogbackappender;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import software.amazon.awssdk.core.AmazonServiceException;
import software.amazon.awssdk.core.AmazonWebServiceRequest;
import software.amazon.awssdk.core.auth.AwsCredentials;
import software.amazon.awssdk.core.auth.AwsCredentialsProvider;
import software.amazon.awssdk.core.auth.DefaultCredentialsProvider;
import software.amazon.awssdk.core.auth.StaticCredentialsProvider;
import software.amazon.awssdk.core.regions.Region;
import software.amazon.awssdk.core.util.EC2MetadataUtils;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;
import software.amazon.awssdk.services.ec2.EC2Client;
import software.amazon.awssdk.services.ec2.model.DescribeTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeTagsResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.TagDescription;

/**
 * CloudWatch log appender for logback.
 * 
 * @author graywatson
 */
public class CloudWatchAppender extends UnsynchronizedAppenderBase<ILoggingEvent>
		implements AppenderAttachable<ILoggingEvent> {

	/** size of batch to write to cloudwatch api */
	private static final int DEFAULT_MAX_BATCH_SIZE = 128;
	/** time in millis to wait until we have a bunch of events to write */
	private static final long DEFAULT_MAX_BATCH_TIME_MILLIS = 5000;
	/** internal event queue size before we drop log requests on the floor */
	private static final int DEFAULT_INTERNAL_QUEUE_SIZE = 8192;
	/** create log destination group and stream when we startup */
	private static final boolean DEFAULT_CREATE_LOG_DESTS = true;
	/** max time to wait in millis before dropping a log event on the floor */
	private static final long DEFAULT_MAX_QUEUE_WAIT_TIME_MILLIS = 100;
	/** time to wait to initialize which helps when application is starting up */
	private static final long DEFAULT_INITIAL_WAIT_TIME_MILLIS = 0;
	/** how many times to retry a cloudwatch request */
	private static final int PUT_REQUEST_RETRY_COUNT = 2;
	/** property looked for to find the aws access-key-id */
	private static final String AWS_ACCESS_KEY_ID_PROPERTY = "cloudwatchappender.aws.accessKeyId";
	/** property looked for to find the aws secret-key */
	private static final String AWS_SECRET_KEY_PROPERTY = "cloudwatchappender.aws.secretKey";

	private String accessKeyId;
	private String secretKey;
	private String region;
	private String logGroup;
	private String logStream;
	private Layout<ILoggingEvent> layout;
	private Appender<ILoggingEvent> emergencyAppender;
	private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
	private long maxBatchTimeMillis = DEFAULT_MAX_BATCH_TIME_MILLIS;
	private long maxQueueWaitTimeMillis = DEFAULT_MAX_QUEUE_WAIT_TIME_MILLIS;
	private int internalQueueSize = DEFAULT_INTERNAL_QUEUE_SIZE;
	private boolean createLogDests = DEFAULT_CREATE_LOG_DESTS;
	private long initialWaitTimeMillis = DEFAULT_INITIAL_WAIT_TIME_MILLIS;

	private CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient;
	private long eventsWrittenCount;

	private BlockingQueue<ILoggingEvent> loggingEventQueue;
	private Thread cloudWatchWriterThread;
	private final ThreadLocal<Boolean> stopMessagesThreadLocal = new ThreadLocal<Boolean>();
	private volatile boolean warningMessagePrinted;
	private final InputLogEventComparator inputLogEventComparator = new InputLogEventComparator();

	public CloudWatchAppender() {
		// for spring
	}

	/**
	 * After all of the setters, call initial to setup the appender.
	 */
	@Override
	public void start() {
		if (started) {
			return;
		}
		/*
		 * NOTE: as we startup here, we can't make any log calls so we can't make any RPC calls or anything without
		 * going recursive.
		 */
		if (MiscUtils.isBlank(region)) {
			throw new IllegalStateException("Region not set or invalid for appender: " + region);
		}
		if (MiscUtils.isBlank(logGroup)) {
			throw new IllegalStateException("Log group name not set or invalid for appender: " + logGroup);
		}
		if (MiscUtils.isBlank(logStream)) {
			throw new IllegalStateException("Log stream name not set or invalid for appender: " + logStream);
		}
		if (layout == null) {
			throw new IllegalStateException("Layout was not set for appender");
		}

		loggingEventQueue = new ArrayBlockingQueue<ILoggingEvent>(internalQueueSize);

		// create our writer thread in the background
		cloudWatchWriterThread = new Thread(new CloudWatchWriter(), getClass().getSimpleName());
		cloudWatchWriterThread.setDaemon(true);
		cloudWatchWriterThread.start();

		if (emergencyAppender != null && !emergencyAppender.isStarted()) {
			emergencyAppender.start();
		}
		super.start();
	}

	@Override
	public void stop() {
		if (!started) {
			return;
		}
		super.stop();

		cloudWatchWriterThread.interrupt();
		try {
			cloudWatchWriterThread.join(1000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (cloudWatchLogsAsyncClient != null) {
			cloudWatchLogsAsyncClient.close();
			cloudWatchLogsAsyncClient = null;
		}
	}

	@Override
	protected void append(ILoggingEvent loggingEvent) {

		// check wiring
		if (loggingEventQueue == null) {
			if (!warningMessagePrinted) {
				System.err.println(getClass().getSimpleName() + " not wired correctly, ignoring all log messages");
				warningMessagePrinted = true;
			}
			return;
		}

		// skip it if we just went recursive
		Boolean stopped = stopMessagesThreadLocal.get();
		if (stopped == null || !stopped) {
			try {
				if (loggingEvent instanceof LoggingEvent && ((LoggingEvent) loggingEvent).getThreadName() == null) {
					// we need to do this so that the right thread gets set in the event
					((LoggingEvent) loggingEvent).setThreadName(Thread.currentThread().getName());
				}
				if (!loggingEventQueue.offer(loggingEvent, maxQueueWaitTimeMillis, TimeUnit.MILLISECONDS)) {
					if (emergencyAppender != null) {
						emergencyAppender.doAppend(loggingEvent);
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	// not-required, default is to use the DefaultAWSCredentialsProviderChain
	public void setAccessKeyId(String accessKeyId) {
		this.accessKeyId = accessKeyId;
	}

	// not-required, default is to use the DefaultAWSCredentialsProviderChain
	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	// required
	public void setRegion(String region) {
		this.region = region;
	}

	// required
	public void setLogGroup(String logGroup) {
		this.logGroup = logGroup;
	}

	// required
	public void setLogStream(String logStream) {
		this.logStream = logStream;
	}

	// required
	public void setLayout(Layout<ILoggingEvent> layout) {
		this.layout = layout;
	}

	// not-required, default is DEFAULT_MAX_BATCH_SIZE
	public void setMaxBatchSize(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}

	// not-required, default is DEFAULT_MAX_BATCH_TIME_MILLIS
	public void setMaxBatchTimeMillis(long maxBatchTimeMillis) {
		this.maxBatchTimeMillis = maxBatchTimeMillis;
	}

	// not-required, default is DEFAULT_MAX_QUEUE_WAIT_TIME_MILLIS
	public void setMaxQueueWaitTimeMillis(long maxQueueWaitTimeMillis) {
		this.maxQueueWaitTimeMillis = maxQueueWaitTimeMillis;
	}

	// not-required, default is DEFAULT_INTERNAL_QUEUE_SIZE
	public void setInternalQueueSize(int internalQueueSize) {
		this.internalQueueSize = internalQueueSize;
	}

	// not-required, default is DEFAULT_CREATE_LOG_DESTS
	public void setCreateLogDests(boolean createLogDests) {
		this.createLogDests = createLogDests;
	}

	// not-required, default is 0
	public void setInitialWaitTimeMillis(long initialWaitTimeMillis) {
		this.initialWaitTimeMillis = initialWaitTimeMillis;
	}

	// not required, for testing purposes
	void setCloudWatchLogsAsyncClient(CloudWatchLogsAsyncClient cloudWatchLogsAsyncClient) {
		this.cloudWatchLogsAsyncClient = cloudWatchLogsAsyncClient;
	}

	// for testing purposes
	long getEventsWrittenCount() {
		return eventsWrittenCount;
	}

	// for testing purposes
	boolean isWarningMessagePrinted() {
		return warningMessagePrinted;
	}

	@Override
	public void addAppender(Appender<ILoggingEvent> appender) {
		if (emergencyAppender == null) {
			emergencyAppender = appender;
		} else {
			addWarn("One and only one appender may be attached to " + getClass().getSimpleName());
			addWarn("Ignoring additional appender named [" + appender.getName() + "]");
		}
	}

	@Override
	public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
		throw new UnsupportedOperationException("Don't know how to create iterator");
	}

	@Override
	public Appender<ILoggingEvent> getAppender(String name) {
		if (emergencyAppender != null && name != null && name.equals(emergencyAppender.getName())) {
			return emergencyAppender;
		} else {
			return null;
		}
	}

	@Override
	public boolean isAttached(Appender<ILoggingEvent> appender) {
		return (emergencyAppender == appender);
	}

	@Override
	public void detachAndStopAllAppenders() {
		if (emergencyAppender != null) {
			emergencyAppender.stop();
			emergencyAppender = null;
		}
	}

	@Override
	public boolean detachAppender(Appender<ILoggingEvent> appender) {
		if (emergencyAppender == appender) {
			emergencyAppender = null;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean detachAppender(String name) {
		if (emergencyAppender != null && emergencyAppender.getName().equals(name)) {
			emergencyAppender = null;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Background thread that writes the log events to cloudwatch.
	 */
	private class CloudWatchWriter implements Runnable {

		private String sequenceToken;
		private boolean initialized;

		@Override
		public void run() {

			try {
				Thread.sleep(initialWaitTimeMillis);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}

			List<ILoggingEvent> events = new ArrayList<ILoggingEvent>(maxBatchSize);
			Thread thread = Thread.currentThread();
			while (!thread.isInterrupted()) {
				long batchTimeout = System.currentTimeMillis() + maxBatchTimeMillis;
				while (!thread.isInterrupted()) {
					long timeoutMillis = batchTimeout - System.currentTimeMillis();
					if (timeoutMillis < 0) {
						break;
					}
					ILoggingEvent loggingEvent;
					try {
						loggingEvent = loggingEventQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						// write what we have and bail
						if (!events.isEmpty()) {
							writeEvents(events);
							events.clear();
						}
						return;
					}
					if (loggingEvent == null) {
						// wait timed out
						break;
					}
					events.add(loggingEvent);
					if (events.size() >= maxBatchSize) {
						// batch size exceeded
						break;
					}
				}
				if (!events.isEmpty()) {
					writeEvents(events);
					events.clear();
				}
			}

			// now clear the queue and write all the rest
			events.clear();
			while (true) {
				ILoggingEvent event = loggingEventQueue.poll();
				if (event == null) {
					// nothing else waiting
					break;
				}
				events.add(event);
				if (events.size() >= maxBatchSize) {
					writeEvents(events);
					events.clear();
				}
			}
			if (!events.isEmpty()) {
				writeEvents(events);
				events.clear();
			}
		}

		private void writeEvents(List<ILoggingEvent> events) {

			if (!initialized) {
				initialized = true;
				Exception exception = null;
				try {
					stopMessagesThreadLocal.set(true);
					if (cloudWatchLogsAsyncClient == null) {
						createLogsClient();
					}
				} catch (Exception e) {
					exception = e;
				} finally {
					stopMessagesThreadLocal.set(false);
				}
				if (exception != null) {
					logError("Problems initializing cloudwatch writer", exception);
				}
			}

			// if we didn't get an aws logs-client then just write to the emergency appender (if any)
			if (cloudWatchLogsAsyncClient == null) {
				appendToEmergencyAppender(events);
				return;
			}

			// we need this in case our RPC calls create log output which we don't want to then log again
			stopMessagesThreadLocal.set(true);
			Exception exception = null;
			try {
				List<InputLogEvent> logEvents = new ArrayList<InputLogEvent>(events.size());
				for (ILoggingEvent event : events) {
					String message = layout.doLayout(event);

					InputLogEvent logEvent =
							InputLogEvent.builder().timestamp(event.getTimeStamp()).message(message).build();
					logEvents.add(logEvent);
				}
				// events must be in sorted order according to AWS otherwise an exception is thrown
				Collections.sort(logEvents, inputLogEventComparator);

				for (int i = 0; i < PUT_REQUEST_RETRY_COUNT; i++) {
					try {

						PutLogEventsRequest.Builder requestBuilder = PutLogEventsRequest.builder().logGroupName(logGroup).logStreamName(logStream).logEvents(logEvents);
						if (sequenceToken != null) {
							requestBuilder.sequenceToken(sequenceToken);
						}
                        CompletableFuture<PutLogEventsResponse> putLogEventsResponseCompletableFuture = cloudWatchLogsAsyncClient.putLogEvents(requestBuilder.build());
                        sequenceToken = putLogEventsResponseCompletableFuture.get().nextSequenceToken();
						exception = null;
						eventsWrittenCount += logEvents.size();
						break;
					} catch (InvalidSequenceTokenException iste) {
						exception = iste;
						sequenceToken = iste.expectedSequenceToken();
					}
				}
			} catch (DataAlreadyAcceptedException daac) {
				exception = daac;
				sequenceToken = daac.expectedSequenceToken();
			} catch (Exception e) {
				// catch everything else to make sure we don't quit the thread
				exception = e;
			} finally {
				if (exception != null) {
					// we do this because we don't want to go recursive
					events.add(makeEvent(Level.ERROR,
							"Exception thrown when creating logging " + events.size() + " events", exception));
					appendToEmergencyAppender(events);
				}
				stopMessagesThreadLocal.set(false);
			}
		}

		private void appendToEmergencyAppender(List<ILoggingEvent> events) {
			if (emergencyAppender != null) {
				try {
					for (ILoggingEvent event : events) {
						emergencyAppender.doAppend(event);
					}
				} catch (Exception e) {
					// oh well, we tried
				}
			}
		}

		private void createLogsClient() throws ExecutionException, InterruptedException {
			AwsCredentialsProvider credentialProvider;
			if (MiscUtils.isBlank(accessKeyId)) {
				// try to use our class properties
				accessKeyId = System.getProperty(AWS_ACCESS_KEY_ID_PROPERTY);
				secretKey = System.getProperty(AWS_SECRET_KEY_PROPERTY);
			}
			if (MiscUtils.isBlank(accessKeyId)) {
				// if we are still blank then use the default credentials provider
				credentialProvider = DefaultCredentialsProvider.builder().build();
			} else {
				credentialProvider = StaticCredentialsProvider.create(AwsCredentials.create(accessKeyId, secretKey));
			}
			cloudWatchLogsAsyncClient = CloudWatchLogsAsyncClient.builder()
					.credentialsProvider(credentialProvider)
					.region(Region.of(region))
					.build();
			verifyLogGroupExists();
			verifyLogStreamExists();
			lookupInstanceName(credentialProvider);
		}

		private void verifyLogGroupExists() throws ExecutionException, InterruptedException {
			DescribeLogGroupsRequest request = DescribeLogGroupsRequest.builder().logGroupNamePrefix(logGroup).build();
			CompletableFuture<DescribeLogGroupsResponse> describeLogGroupsResponseCompletableFuture = cloudWatchLogsAsyncClient.describeLogGroups(request);
			DescribeLogGroupsResponse describeLogGroupsResponse = describeLogGroupsResponseCompletableFuture.get();
			for (LogGroup group : describeLogGroupsResponse.logGroups()) {
				if (logGroup.equals(group.logGroupName())) {
					return;
				}
			}
			if (createLogDests) {
				callLogClientMethod("createLogGroup", CreateLogGroupRequest.builder().logGroupName(logGroup).build());
			} else {
				logWarn("Log-group '" + logGroup + "' doesn't exist and not created", null);
			}
		}

		private void verifyLogStreamExists() throws ExecutionException, InterruptedException {
			DescribeLogStreamsRequest request =
					DescribeLogStreamsRequest.builder().logGroupName(logGroup).logStreamNamePrefix(logStream).build();
			CompletableFuture<DescribeLogStreamsResponse> describeLogStreamsResponseCompletableFuture = cloudWatchLogsAsyncClient.describeLogStreams(request);
			DescribeLogStreamsResponse describeLogStreamsResponse = describeLogStreamsResponseCompletableFuture.get();
			for (LogStream stream : describeLogStreamsResponse.logStreams()) {
				if (logStream.equals(stream.logStreamName())) {
					sequenceToken = stream.uploadSequenceToken();
					return;
				}
			}
			if (createLogDests) {
				callLogClientMethod("createLogStream", CreateLogStreamRequest.builder().logGroupName(logGroup).logStreamName(logStream).build());
			} else {
				logWarn("Log-stream '" + logStream + "' doesn't exist and not created", null);
			}
		}

		/**
		 * This is a hack to work around the problems that were introduced when the appender was compiled with AWS SDK
		 * version 1.9 or 1.10 but the user was running with version 1.11.
		 * 
		 * The problem was that the createLogStream() method added a return object somewhere between 1.10 and 1.11 which
		 * broke backwards compatibility and the applications would throw NoSuchMethodError. Using reflection causes the
		 * linkage to be weaker and seems to work.
		 */
		private void callLogClientMethod(String methodName, AmazonWebServiceRequest arg) {
			try {
				Method method = cloudWatchLogsAsyncClient.getClass().getMethod(methodName, arg.getClass());
				method.invoke(cloudWatchLogsAsyncClient, arg);
				logInfo("Created: " + arg);
			} catch (Exception e) {
				logError("Problems creating: " + arg, e);
			}
		}

		private void lookupInstanceName(AwsCredentialsProvider credentialProvider) {
			String instanceId = EC2MetadataUtils.getInstanceId();
			if (instanceId == null) {
				return;
			}
			Ec2InstanceIdConverter.setInstanceId(instanceId);
			EC2Client ec2Client = null;
			try {
				ec2Client = EC2Client.builder()
						.credentialsProvider(credentialProvider)
				        .region(Region.of(region))
						.build();
				DescribeTagsRequest request = DescribeTagsRequest.builder()
				.filters(Arrays.asList(
						Filter.builder().name("resource-type").values("instance").build(),
						Filter.builder().name("resource-id").values(instanceId).build())
				).build();
				DescribeTagsResponse describeTagsResponse = ec2Client.describeTags(request);
				List<TagDescription> tags = describeTagsResponse.tags();
				for (TagDescription tag : tags) {
					if ("Name".equals(tag.key())) {
						Ec2InstanceNameConverter.setInstanceName(tag.value());
						return;
					}
				}
				logInfo("Could not find EC2 instance name in tags: " + tags);
			} catch (AmazonServiceException ase) {
				logWarn("Looking up EC2 instance-name threw", ase);
			} finally {
				if (ec2Client != null) {
					ec2Client.close();
				}
			}
			// if we can't lookup the instance name then set it as the instance-id
			Ec2InstanceNameConverter.setInstanceName(instanceId);
		}

		private void logInfo(String message) {
			appendEvent(Level.INFO, message, null);
		}

		private void logWarn(String message, Throwable th) {
			appendEvent(Level.WARN, message, th);
		}

		private void logError(String message, Throwable th) {
			appendEvent(Level.ERROR, message, th);
		}

		private void appendEvent(Level level, String message, Throwable th) {
			append(makeEvent(level, message, th));
		}

		private LoggingEvent makeEvent(Level level, String message, Throwable th) {
			LoggingEvent event = new LoggingEvent();
			event.setLoggerName(CloudWatchAppender.class.getName());
			event.setLevel(level);
			event.setMessage(message);
			event.setTimeStamp(System.currentTimeMillis());
			if (th != null) {
				event.setThrowableProxy(new ThrowableProxy(th));
			}
			return event;
		}
	}

	/**
	 * Compares a log event by it's timestamp value.
	 */
	private static class InputLogEventComparator implements Comparator<InputLogEvent> {
		@Override
		public int compare(InputLogEvent o1, InputLogEvent o2) {
			if (o1.timestamp() == null) {
				if (o2.timestamp() == null) {
					return 0;
				} else {
					// null - long
					return -1;
				}
			} else if (o2.timestamp() == null) {
				// long - null
				return 1;
			} else {
				return o1.timestamp().compareTo(o2.timestamp());
			}
		}
	}
}
