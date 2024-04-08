package com.easy2excel;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.rekognition.model.DetectFacesRequest;
import com.amazonaws.services.rekognition.model.DetectFacesResult;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Face;
import com.amazonaws.services.rekognition.model.FaceDetail;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class LambdaHandler implements RequestHandler<S3Event, String> {
	private static final String REGION = "us-east-2";

	private static final String DYNAMODB_TABLE_NAME = "facerecognition";

	AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(REGION))
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();

	AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.fromName(REGION))
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
	DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
	AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion(REGION)
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();

	@Override
	public String handleRequest(S3Event s3Event, Context context) {
		final LambdaLogger logger = context.getLogger();

		if (s3Event.getRecords().isEmpty()) {
			logger.log("No records found");
			return "no record found";
		}
		Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);

		for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
			String bucketName = record.getS3().getBucket().getName();
			String objectKey = record.getS3().getObject().getKey();

			// 1. we create S3 client
			// 2. invoking GetObject
			// 3. processing the InputStream from S3

			S3Object s3Object = s3client.getObject(bucketName, objectKey);
			S3ObjectInputStream inputStream = s3Object.getObjectContent();
			byte[] imageBytes;
			try {
				imageBytes = IOUtils.toByteArray(inputStream);
			} catch (IOException e) {
				logger.log("Error occurred while reading image from S3: " + e.getMessage());
				continue; // Skip processing this image
			}
			context.getLogger().log("BucketName ::: " + bucketName);
			context.getLogger().log("objectKey ::: " + objectKey);

            Image image = new Image().withBytes(ByteBuffer.wrap(imageBytes));
            DetectFacesRequest request = new DetectFacesRequest().withImage(image);
            DetectFacesResult result = rekognitionClient.detectFaces(request);
            List<FaceDetail> faceDetails= result.getFaceDetails();

			// 3. Store labels in DynamoDB
            StringBuilder facePrintsBuilder = new StringBuilder();
            for (FaceDetail  faceDetail: faceDetails) {
                // Assuming you want to store face print details
                facePrintsBuilder.append(faceDetail.toString()).append(", ");
            }
            String facePrints = facePrintsBuilder.toString();

            Item item = new Item()
                    .withPrimaryKey("RekognitionId", facePrints)
                    .withString("FacePrintsName",objectKey );
            table.putItem(item);


			logger.log("Stored in DynamoDB: " + DYNAMODB_TABLE_NAME);
		}
		return "successfully read file from s3 bucket";

	}
}
