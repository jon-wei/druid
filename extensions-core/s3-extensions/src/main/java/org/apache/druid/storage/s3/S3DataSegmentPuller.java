/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.CompressionUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.URIDataPuller;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * A data segment puller that also hanldes URI data pulls.
 */
public class S3DataSegmentPuller implements URIDataPuller
{
  public static final int DEFAULT_RETRY_COUNT = 3;

  private static final Logger log = new Logger(S3DataSegmentPuller.class);

  protected static final String BUCKET = "bucket";
  protected static final String KEY = "key";

  protected final ServerSideEncryptingAmazonS3 s3Client;

  @Inject
  public S3DataSegmentPuller(ServerSideEncryptingAmazonS3 s3Client)
  {
    this.s3Client = s3Client;
  }

  FileUtils.FileCopyResult getSegmentFiles(final String bucket, final String key, final File outDir)
      throws SegmentLoadingException
  {
    log.info("Pulling index at bucket[%s], key[%s] to outDir[%s]", bucket, key, outDir);

    if (!isObjectInBucket(bucket, key)) {
      throw new SegmentLoadingException("IndexFile[%s][%s] does not exist.", bucket, key);
    }

    try {
      org.apache.commons.io.FileUtils.forceMkdir(outDir);

      final GetObjectRequest request = new GetObjectRequest(bucket, key);
      final ByteSource byteSource = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          try {
            final S3Object s3Object = s3Client.getObject(request);
            return getInputStreamFromS3Object(s3Object);
          }
          catch (AmazonServiceException e) {
            if (S3Utils.S3RETRY.apply(e)) {
              throw new IOException("Recoverable exception", e);
            }
            throw Throwables.propagate(e);
          }
        }
      };
      if (CompressionUtils.isZip(key)) {
        final FileUtils.FileCopyResult result = CompressionUtils.unzip(
            byteSource,
            outDir,
            S3Utils.S3RETRY,
            false
        );
        log.info(
            "Loaded %d bytes from bucket[%s], key[%s] to [%s]",
            result.size(),
            request.getBucketName(),
            key,
            outDir.getAbsolutePath()
        );
        return result;
      }
      if (CompressionUtils.isGz(key)) {
        final String fname = Files.getNameWithoutExtension(key);
        final File outFile = new File(outDir, fname);

        final FileUtils.FileCopyResult result = CompressionUtils.gunzip(byteSource, outFile, S3Utils.S3RETRY);
        log.info(
            "Loaded %d bytes from bucket[%s], key[%s] to [%s]",
            result.size(),
            request.getBucketName(),
            key,
            outFile.getAbsolutePath()
        );
        return result;
      }
      throw new IAE("Do not know how to load file type at bucket[%s], key[%s]", bucket, key);
    }

    catch (Exception e) {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        log.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from bucket[%s], key[%s]",
            outDir.getAbsolutePath(),
            bucket,
            key
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    try {
      final AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
      final S3Object s3Object = s3Client.getObject(amazonS3URI.getBucket(), amazonS3URI.getKey());
      return getInputStreamFromS3Object(s3Object);
    }
    catch (AmazonServiceException e) {
      throw new IOE(e, "Could not load URI [%s]", uri);
    }
  }

  private InputStream getInputStreamFromS3Object(final S3Object s3Object) throws AmazonServiceException
  {
    final InputStream in = s3Object.getObjectContent();
    final Closer closer = Closer.create();
    closer.register(in);
    closer.register(s3Object);

    return new FilterInputStream(in)
    {
      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Override
  public Predicate<Throwable> shouldRetryPredicate()
  {
    // Yay! smart retries!
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        if (e == null) {
          return false;
        }
        if (e instanceof AmazonServiceException) {
          return S3Utils.isServiceExceptionRecoverable((AmazonServiceException) e);
        }
        if (S3Utils.S3RETRY.apply(e)) {
          return true;
        }
        // Look all the way down the cause chain, just in case something wraps it deep.
        return apply(e.getCause());
      }
    };
  }

  /**
   * Returns the "version" (aka last modified timestamp) of the URI
   *
   * @param uri The URI to check the last timestamp
   *
   * @return The time in ms of the last modification of the URI in String format
   *
   * @throws IOException
   */
  @Override
  public String getVersion(URI uri) throws IOException
  {
    try {
      final AmazonS3URI amazonS3URI = new AmazonS3URI(uri);
      final S3ObjectSummary objectSummary = S3Utils.getSingleObjectSummary(
          s3Client,
          amazonS3URI.getBucket(),
          amazonS3URI.getKey()
      );
      return StringUtils.format("%d", objectSummary.getLastModified().getTime());
    }
    catch (AmazonServiceException e) {
      if (S3Utils.isServiceExceptionRecoverable(e)) {
        // The recoverable logic is always true for IOException, so we want to only pass IOException if it is recoverable
        throw new IOE(e, "Could not fetch last modified timestamp from URI [%s]", uri);
      } else {
        throw new RE(e, "Error fetching last modified timestamp from URI [%s]", uri);
      }
    }
  }

  private boolean isObjectInBucket(final String bucket, final String key) throws SegmentLoadingException
  {
    try {
      return S3Utils.retryS3Operation(
          () -> S3Utils.isObjectInBucketIgnoringPermission(s3Client, bucket, key)
      );
    }
    catch (AmazonS3Exception | IOException e) {
      throw new SegmentLoadingException(e, "S3 fail! Bucket[%s] Key[%s]", bucket, key);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
