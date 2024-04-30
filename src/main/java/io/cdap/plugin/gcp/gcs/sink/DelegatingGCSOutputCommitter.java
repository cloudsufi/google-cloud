/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.gcs.sink;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Output Committer which creates and delegates operations to other GCS Output Committer instances.
 *
 * Delegated instances are created based on a supplied Output Format and Destination Table Names.
 */
public class DelegatingGCSOutputCommitter extends OutputCommitter {
  private final Map<String, OutputCommitter> committerMap;
  private TaskAttemptContext tc;

  public DelegatingGCSOutputCommitter() {
    committerMap = new HashMap<>();
  }

  // Set Task Context
  public void setTaskContext(TaskAttemptContext taskContext) {
    tc = taskContext;
  }

  /**
   * Add a new GCSOutputCommitter based on a supplied Output Format and Table Name.
   *
   * This GCS Output Committer gets initialized when created.
   */
  @SuppressWarnings("rawtypes")
  public void addGCSOutputCommitterFromOutputFormat(OutputFormat outputFormat,
                                                    TaskAttemptContext context,
                                                    String tableName) throws IOException, InterruptedException {
    //Set output directory
    context.getConfiguration().set(FileOutputFormat.OUTDIR,
                                   DelegatingGCSOutputUtils.buildOutputPath(context.getConfiguration(), tableName));

    //Wrap output committer into the GCS Output Committer.
    GCSOutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(context));

    //Initialize the new GCS Output Committer and add it to the Committer Map
    gcsOutputCommitter.setupJob(context);
    gcsOutputCommitter.setupTask(context);
    committerMap.put(tableName, gcsOutputCommitter);
    writePartitionFile(context.getConfiguration().get(FileOutputFormat.OUTDIR), context);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    Path tempPath = new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME);
    fs.mkdirs(tempPath);
  }

  private void writePartitionFile(String path, TaskAttemptContext context) throws IOException {
    Path outputPath = new Path(context.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    Path tempPath = new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME);
    FileSystem fs = tempPath.getFileSystem(context.getConfiguration());
    String taskId = context.getTaskAttemptID().getTaskID().toString();
    Path taskPartitionFile = new Path(tempPath, taskId + "_partitions.txt");
    if (!fs.exists(taskPartitionFile)) {
      fs.createNewFile(taskPartitionFile);
    }
    DataOutputStream out = fs.append(taskPartitionFile);
    out.writeBytes(path + "\n");
    out.close();
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //no-op
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    if (committerMap.isEmpty()) {
      return false;
    }

    boolean needsTaskCommit = true;

    for (OutputCommitter committer : committerMap.values()) {
      needsTaskCommit = needsTaskCommit && committer.needsTaskCommit(taskAttemptContext);
    }

    return needsTaskCommit;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.commitTask(taskAttemptContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    Path tempPath = new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME);
    Set<String> outputPaths = new HashSet<>();

    for (FileStatus status : fs.listStatus(tempPath)) {
      if (status.getPath().getName().endsWith("_partitions.txt")) {
        FSDataInputStream dis =  fs.open(status.getPath());
        DataInputStream in = new DataInputStream(new BufferedInputStream(dis));
        BufferedReader br = new BufferedReader(new java.io.InputStreamReader(in));
        String line;
        while ((line = br.readLine()) != null) {
          outputPaths.add(line);
        }
        in.close();
      }
    }
    for (String output : outputPaths) {
      tc.getConfiguration().set(FileOutputFormat.OUTDIR, output);
      FileOutputCommitter committer = new FileOutputCommitter(new Path(output), tc);
      committer.commitJob(jobContext);
    }
    cleanupJob(jobContext);
  }


  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    fs.delete(new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME), true);
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    IOException ioe = null;

    for (OutputCommitter committer : committerMap.values()) {
      try {
        committer.abortTask(taskAttemptContext);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    IOException ioe = null;
    cleanupJob(jobContext);

    for (OutputCommitter committer : committerMap.values()) {
      try {
        committer.abortJob(jobContext, state);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }
}
