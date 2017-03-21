/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.committer

import java.io.IOException

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import org.apache.spark.internal.Logging

/**
 * Optimization for Spark
 */
class SparkFileOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends FileOutputCommitter(outputPath, context) with Logging {

  override def commitJob(jobContext: JobContext): Unit = {

    if (hasOutputPath) {
      val finalOutput: Path = outputPath
      val fs: FileSystem = finalOutput.getFileSystem(context.getConfiguration)
//      for (stat <- getAllCommittedTaskPaths(context)) {
//        mergePaths(fs, stat, finalOutput)
//      }
      val taskPath = getCommittedTaskPath(context)
      mergePaths(fs, taskPath, finalOutput)
      cleanupJob(context)
      if (context.getConfiguration.getBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
        true)) {
        val markerPath: Path = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
        fs.create(markerPath).close
      }
    } else {
      logWarning("Output Path is null in commitJob()")
    }
  }

  override def commitTask(context: TaskAttemptContext, taskAttemptPath: Path): Unit = {
    if (hasOutputPath()) {
      context.progress()
    }
  }

  def hasOutputPath(): Boolean = outputPath != null

  def mergePaths(fs: FileSystem, from: Path, to: Path): Unit = {
    logDebug(s"Merging data from $from to $to")
      if (fs.exists(to)) {
        if (!fs.delete(to, true)) {
          throw new IOException(s"Failed to delete ${to}")
        }
      }
      if (!fs.rename(fs.getFileStatus(from).getPath, to)) {
        throw new IOException("Failed to rename " + from + " to " + to)
      }
  }

}