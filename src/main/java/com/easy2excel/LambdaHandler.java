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
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectFacesRequest;
import com.amazonaws.services.rekognition.model.DetectFacesResult;
import com.amazonaws.services.rekognition.model.FaceDetail;
import com.amazonaws.services.rekognition.model.FaceMatch;
import com.amazonaws.services.rekognition.model.FaceRecord;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.IndexFacesRequest;
import com.amazonaws.services.rekognition.model.IndexFacesResult;
import com.amazonaws.services.rekognition.model.SearchFacesByImageRequest;
import com.amazonaws.services.rekognition.model.SearchFacesByImageResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class LambdaHandler implements RequestHandler<S3Event, String> {
	private static final String REGION = "us-east-1";

	private static final String DYNAMODB_TABLE_NAME = "facerecognition";

	AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(REGION))
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();

	AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.fromName(REGION))
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
	DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
	AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion(REGION)
			.withCredentials(new DefaultAWSCredentialsProviderChain()).build();
	String collectionId = "imageComparision";

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

			S3Object s3Object = s3client.getObject(bucketName, objectKey);
			S3ObjectInputStream inputStream = s3Object.getObjectContent();
			byte[] imageBytes;
			try {
				imageBytes = IOUtils.toByteArray(inputStream);
			} catch (IOException e) {
				logger.log("Error occurred while reading image from S3: " + e.getMessage());
				continue;
			}
			context.getLogger().log("BucketName ::: " + bucketName);
			context.getLogger().log("objectKey ::: " + objectKey);

			Image image = new Image().withBytes(ByteBuffer.wrap(imageBytes));
			DetectFacesRequest request = new DetectFacesRequest().withImage(image);
			DetectFacesResult result;

			try {
				result = rekognitionClient.detectFaces(request);
			} catch (AmazonRekognitionException e) {
				context.getLogger().log("Error occurred while detecting faces: " + e.getMessage());
				continue;
			}
			IndexFacesRequest indexRequest = new IndexFacesRequest().withCollectionId(collectionId).withImage(image)
					.withExternalImageId(objectKey);
			IndexFacesResult indexResult;
			try {
				indexResult = rekognitionClient.indexFaces(indexRequest);
			} catch (AmazonRekognitionException e) {
				context.getLogger().log("Error occurred while indexing faces: " + e.getMessage());
				continue;
			}
			List<FaceRecord> faceRecords = indexResult.getFaceRecords();
			SearchFacesByImageRequest searchRequest = new SearchFacesByImageRequest().withCollectionId(collectionId)
					.withImage(image);

			SearchFacesByImageResult searchResult;
			try {
				searchResult = rekognitionClient.searchFacesByImage(searchRequest);
			} catch (AmazonRekognitionException e) {
				context.getLogger().log("Error occurred while searching faces: " + e.getMessage());
				continue;
			}
			List<FaceMatch> faceMatches = searchResult.getFaceMatches();
			if (!faceMatches.isEmpty()) {
				for (FaceMatch match : faceMatches) {
					float similarity = match.getSimilarity();
					String matchedFaceId = match.getFace().getFaceId();
					context.getLogger().log("Face matched with similarity: " + similarity);
				}
			} else {
				context.getLogger().log("No matching faces found in the collection.");
			}
			List<FaceDetail> faceDetails = result.getFaceDetails();
			if (faceDetails.isEmpty()) {
				context.getLogger().log("No faces detected in the image");
				continue;
			}

			StringBuilder facePrintsBuilder = new StringBuilder();
			context.getLogger().log("StringBuilder ::: " + facePrintsBuilder);

			for (FaceDetail faceDetail : faceDetails) {
				facePrintsBuilder.append(faceDetail.toString()).append(", ");
				context.getLogger().log("faceDetail ::: " + faceDetail);
				context.getLogger().log("StringBuilder ::: " + facePrintsBuilder);

			}
			String facePrints = facePrintsBuilder.toString().trim();
			context.getLogger().log("facePrints ::: " + facePrints + "---------example");
			context.getLogger().log("item is started ::: " + "item");

			
			for (FaceRecord faceRecord : faceRecords) {
			    try {
			        // Extract the recognized face ID
			        String recognizedFaceId = faceRecord.getFace().getFaceId();
					context.getLogger().log("recognizedFaceId ::: " + recognizedFaceId);

			        // Store the recognized face ID along with other information in DynamoDB
			        Item item = new Item()
			            .withPrimaryKey("RekognitionId", recognizedFaceId) // Store recognized face ID as RekognitionId
			            .withString("FacePrintsName", objectKey); // Store the objectKey as FacePrintsName
			        table.putItem(item);

			        context.getLogger().log("Stored in DynamoDB: RekognitionId=" + recognizedFaceId + ", FacePrintsName=" + objectKey);
			    } catch (AmazonRekognitionException e) {
			        context.getLogger().log("Error occurred while indexing faces: " + e.getMessage());
			        continue;
			    }
			}

			
			//Item item = new Item().withPrimaryKey("RekognitionId", facePrints).withString("FacePrintsName", objectKey);
			//table.putItem(item);

			
			
			
			
			context.getLogger().log("item is ended ::: " + "item");
			context.getLogger().log("Stored in DynamoDB ::: " + DYNAMODB_TABLE_NAME);

			logger.log("Stored in DynamoDB: " + DYNAMODB_TABLE_NAME);
		}
		return "successfully read file from s3 bucket";

	}
}
