package com.gpuflight.agent;

import com.gpuflight.agent.model.ArchiverConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.file.Path;

public class LogArchiver {
    private final ArchiverConfig config;
    private final S3Client s3;

    public LogArchiver(ArchiverConfig config) {
        this.config = config;
        this.s3 = S3Client.builder()
            .endpointOverride(URI.create(config.endpoint()))
            .region(Region.of(config.region()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.accessKey(), config.secretKey())
            ))
            .build();
    }

    public void archive(Path file, String objectKey) {
        var request = PutObjectRequest.builder()
            .bucket(config.bucket())
            .key(objectKey)
            .build();
        // Blocking S3 upload is fine on a virtual thread.
        s3.putObject(request, file);
        System.out.println("[archiver] Uploaded " + file.getFileName() + " → s3://" + config.bucket() + "/" + objectKey);
        if (config.deleteAfterUpload()) {
            file.toFile().delete();
            System.out.println("[archiver] Deleted " + file.getFileName());
        }
    }
}
