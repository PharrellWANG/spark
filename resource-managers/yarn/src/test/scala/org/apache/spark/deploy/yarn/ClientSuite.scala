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

package org.apache.spark.deploy.yarn

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.URI
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration
import org.apache.hadoop.yarn.util.Records
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite, TestUtils}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.config._
import org.apache.spark.util.{ResetSystemProperties, SparkConfWithEnv, Utils}

class ClientSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll
  with ResetSystemProperties {

  import Client._

  var oldSystemProperties: Properties = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    oldSystemProperties = SerializationUtils.clone(System.getProperties)
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  override def afterAll(): Unit = {
    try {
      System.setProperties(oldSystemProperties)
      oldSystemProperties = null
    } finally {
      super.afterAll()
    }
  }

  test("default Yarn application classpath") {
    getDefaultYarnApplicationClasspath should be(Some(Fixtures.knownDefYarnAppCP))
  }

  test("default MR application classpath") {
    getDefaultMRApplicationClasspath should be(Some(Fixtures.knownDefMRAppCP))
  }

  test("resultant classpath for an application that defines a classpath for YARN") {
    withAppConf(Fixtures.mapYARNAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(Fixtures.knownYARNAppCP, getDefaultMRApplicationClasspath))
    }
  }

  test("resultant classpath for an application that defines a classpath for MR") {
    withAppConf(Fixtures.mapMRAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(
        flatten(getDefaultYarnApplicationClasspath, Fixtures.knownMRAppCP))
    }
  }

  test("resultant classpath for an application that defines both classpaths, YARN and MR") {
    withAppConf(Fixtures.mapAppConf) { conf =>
      val env = newEnv
      populateHadoopClasspath(conf, env)
      classpath(env) should be(flatten(Fixtures.knownYARNAppCP, Fixtures.knownMRAppCP))
    }
  }

  private val SPARK = "local:/sparkJar"
  private val USER = "local:/userJar"
  private val ADDED = "local:/addJar1,local:/addJar2,/addJar3"

  private val PWD =
    if (classOf[Environment].getMethods().exists(_.getName == "$$")) {
      "{{PWD}}"
    } else if (Utils.isWindows) {
      "%PWD%"
    } else {
      Environment.PWD.$()
    }

  test("Local jar URIs") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq(SPARK))
      .set(USER_CLASS_PATH_FIRST, true)
      .set("spark.yarn.dist.jars", ADDED)
    val env = new MutableHashMap[String, String]()
    val args = new ClientArguments(Array("--jar", USER))

    populateClasspath(args, conf, sparkConf, env)

    val cp = env("CLASSPATH").split(":|;|<CPS>")
    s"$SPARK,$USER,$ADDED".split(",").foreach({ entry =>
      val uri = new URI(entry)
      if (LOCAL_SCHEME.equals(uri.getScheme())) {
        cp should contain (uri.getPath())
      } else {
        cp should not contain (uri.getPath())
      }
    })
    cp should contain(PWD)
    cp should contain (s"$PWD${Path.SEPARATOR}${LOCALIZED_CONF_DIR}")
    cp should not contain (APP_JAR)
  }

  test("Jar path propagation through SparkConf") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq(SPARK))
      .set("spark.yarn.dist.jars", ADDED)
    val client = createClient(sparkConf, args = Array("--jar", USER))
    doReturn(new Path("/")).when(client).copyFileToRemote(any(classOf[Path]),
      any(classOf[Path]), anyShort(), anyBoolean(), any())

    val tempDir = Utils.createTempDir()
    try {
      // Because we mocked "copyFileToRemote" above to avoid having to create fake local files,
      // we need to create a fake config archive in the temp dir to avoid having
      // prepareLocalResources throw an exception.
      new FileOutputStream(new File(tempDir, LOCALIZED_CONF_ARCHIVE)).close()

      client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)
      sparkConf.get(APP_JAR) should be (Some(USER))

      // The non-local path should be propagated by name only, since it will end up in the app's
      // staging dir.
      val expected = ADDED.split(",")
        .map(p => {
          val uri = new URI(p)
          if (LOCAL_SCHEME == uri.getScheme()) {
            p
          } else {
            Option(uri.getFragment()).getOrElse(new File(p).getName())
          }
        })
        .mkString(",")

      sparkConf.get(SECONDARY_JARS) should be (Some(expected.split(",").toSeq))
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("Cluster path translation") {
    val conf = new Configuration()
    val sparkConf = new SparkConf()
      .set(SPARK_JARS, Seq("local:/localPath/spark.jar"))
      .set(GATEWAY_ROOT_PATH, "/localPath")
      .set(REPLACEMENT_ROOT_PATH, "/remotePath")

    getClusterPath(sparkConf, "/localPath") should be ("/remotePath")
    getClusterPath(sparkConf, "/localPath/1:/localPath/2") should be (
      "/remotePath/1:/remotePath/2")

    val env = new MutableHashMap[String, String]()
    populateClasspath(null, conf, sparkConf, env, extraClassPath = Some("/localPath/my1.jar"))
    val cp = classpath(env)
    cp should contain ("/remotePath/spark.jar")
    cp should contain ("/remotePath/my1.jar")
  }

  test("configuration and args propagate through createApplicationSubmissionContext") {
    val conf = new Configuration()
    // When parsing tags, duplicates and leading/trailing whitespace should be removed.
    // Spaces between non-comma strings should be preserved as single tags. Empty strings may or
    // may not be removed depending on the version of Hadoop being used.
    val sparkConf = new SparkConf()
      .set(APPLICATION_TAGS.key, ",tag1, dup,tag2 , ,multi word , dup")
      .set(MAX_APP_ATTEMPTS, 42)
      .set("spark.app.name", "foo-test-app")
      .set(QUEUE_NAME, "staging-queue")
    val args = new ClientArguments(Array())

    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    val getNewApplicationResponse = Records.newRecord(classOf[GetNewApplicationResponse])
    val containerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    val client = new Client(args, conf, sparkConf)
    client.createApplicationSubmissionContext(
      new YarnClientApplication(getNewApplicationResponse, appContext),
      containerLaunchContext)

    appContext.getApplicationName should be ("foo-test-app")
    appContext.getQueue should be ("staging-queue")
    appContext.getAMContainerSpec should be (containerLaunchContext)
    appContext.getApplicationType should be ("SPARK")
    appContext.getClass.getMethods.filter(_.getName.equals("getApplicationTags")).foreach{ method =>
      val tags = method.invoke(appContext).asInstanceOf[java.util.Set[String]]
      tags should contain allOf ("tag1", "dup", "tag2", "multi word")
      tags.asScala.count(_.nonEmpty) should be (4)
    }
    appContext.getMaxAppAttempts should be (42)
  }

  test("Dynamic set spark.dynamicAllocation.maxExecutors if dynamicAllocation enabled") {
    val a = CapacitySchedulerConfiguration.ROOT + ".a"
    val b = CapacitySchedulerConfiguration.ROOT + ".b"
    val a1 = a + ".a1"
    val a2 = a + ".a2"

    val aCapacity = 40F
    val aMaximumCapacity = 60F
    val bCapacity = 60F
    val bMaximumCapacity = 100F
    val a1Capacity = 30F
    val a1MaximumCapacity = 70F
    val a2Capacity = 70F

    val cpuCores = 8
    val numNodeManagers = 10
    val coresTotal = cpuCores * numNodeManagers

    val conf = new CapacitySchedulerConfiguration()

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, Array("a", "b"))
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100)
    conf.setCapacity(a, aCapacity)
    conf.setMaximumCapacity(a, aMaximumCapacity)
    conf.setCapacity(b, bCapacity)
    conf.setMaximumCapacity(b, bMaximumCapacity)

    // Define 2nd-level queues
    conf.setQueues(a, Array("a1", "a2"))
    conf.setCapacity(a1, a1Capacity)
    conf.setMaximumCapacity(a1, a1MaximumCapacity)
    conf.setCapacity(a2, a2Capacity)
    conf.set("yarn.nodemanager.resource.cpu-vcores", cpuCores.toString)

    val yarnCluster = new MiniYARNCluster(classOf[ClientSuite].getName, numNodeManagers, 1, 1)
    yarnCluster.init(conf)
    yarnCluster.start()

    val args = new ClientArguments(Array())

    // dynamicAllocation enabled
    // a's cores: 80 * 0.6 = 48
    val aCoreTotal = (coresTotal * (aMaximumCapacity / 100)).toInt
    val sparkConfA = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set(QUEUE_NAME, "a")
    val clientA = new Client(args, yarnCluster.getConfig, sparkConfA)

    assert(Int.MaxValue === clientA.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientA.init()
    assert(aCoreTotal === 48)
    assert(clientA.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === 48)
    clientA.stop()

    // a1's cores: 80 * 0.6 * 0.7 = 33
    val a1CoreTotal = (coresTotal * (aMaximumCapacity / 100) * (a1MaximumCapacity/ 100)).toInt
    val sparkConfA1 = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set(QUEUE_NAME, "a1")
    val clientA1 = new Client(args, yarnCluster.getConfig, sparkConfA1)

    assert(Int.MaxValue === clientA1.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientA1.init()
    assert(a1CoreTotal === 33)
    assert(clientA1.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === 33)
    clientA1.stop()

    // a2's cores: 80 * 0.6 * 1 = 48
    val a2CoreTotal = (coresTotal * (aMaximumCapacity / 100) * 1).toInt
    val sparkConfA2 = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set(QUEUE_NAME, "a2")
    val clientA2 = new Client(args, yarnCluster.getConfig, sparkConfA2)

    assert(Int.MaxValue === clientA2.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientA2.init()
    assert(a2CoreTotal === 48)
    assert(clientA2.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === 48)
    clientA2.stop()

    // b's cores: 80 * 1 = 80
    val bCoreTotal = (coresTotal * (bMaximumCapacity / 100)).toInt
    val sparkConfB = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set(QUEUE_NAME, "b")
    val clientB = new Client(args, yarnCluster.getConfig, sparkConfB)

    assert(Int.MaxValue === clientB.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientB.init()
    assert(bCoreTotal === 80)
    assert(clientB.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS) === 80)
    clientB.stop()

    // dynamicAllocation enabled and user set spark.dynamicAllocation.maxExecutors
    val maxExecutors = 3
    val sparkConfSetExecutors = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.dynamicAllocation.maxExecutors", maxExecutors.toString)
      .set(QUEUE_NAME, "b")
    val clientSetExecutors = new Client(args, yarnCluster.getConfig, sparkConfSetExecutors)

    assert(maxExecutors === clientSetExecutors.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientSetExecutors.init()
    assert(maxExecutors === clientSetExecutors.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientSetExecutors.stop()

    // dynamicAllocation enabled and user set spark.executor.cores
    // b's execores = 80 * 1 / 3 = 26
    val executorCores = 3
    val bExecutorTotal = (coresTotal * (bMaximumCapacity / 100)).toInt / executorCores
    val sparkConfSetCores = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.executor.cores", executorCores.toString)
      .set(QUEUE_NAME, "b")
    val clientEnabledSetCores = new Client(args, yarnCluster.getConfig, sparkConfSetCores)

    assert(Int.MaxValue == clientEnabledSetCores.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientEnabledSetCores.init()
    assert(bExecutorTotal === 26)
    assert(26 ===  clientEnabledSetCores.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientEnabledSetCores.stop()

    // dynamicAllocation disabled
    val sparkConfDisabled = new SparkConf()
      .set("spark.dynamicAllocation.enabled", "false")
      .set(QUEUE_NAME, "b")
    val clientDisabled = new Client(args, yarnCluster.getConfig, sparkConfDisabled)

    assert(Int.MaxValue === clientDisabled.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientDisabled.init()
    assert(Int.MaxValue === clientDisabled.sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS))
    clientDisabled.stop()

    yarnCluster.stop()
  }

  test("spark.yarn.jars with multiple paths and globs") {
    val libs = Utils.createTempDir()
    val single = Utils.createTempDir()
    val jar1 = TestUtils.createJarWithFiles(Map(), libs)
    val jar2 = TestUtils.createJarWithFiles(Map(), libs)
    val jar3 = TestUtils.createJarWithFiles(Map(), single)
    val jar4 = TestUtils.createJarWithFiles(Map(), single)

    val jarsConf = Seq(
      s"${libs.getAbsolutePath()}/*",
      jar3.getPath(),
      s"local:${jar4.getPath()}",
      s"local:${single.getAbsolutePath()}/*")

    val sparkConf = new SparkConf().set(SPARK_JARS, jarsConf)
    val client = createClient(sparkConf)

    val tempDir = Utils.createTempDir()
    client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)

    assert(sparkConf.get(SPARK_JARS) ===
      Some(Seq(s"local:${jar4.getPath()}", s"local:${single.getAbsolutePath()}/*")))

    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar1.toURI())), anyShort(),
      anyBoolean(), any())
    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar2.toURI())), anyShort(),
      anyBoolean(), any())
    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(jar3.toURI())), anyShort(),
      anyBoolean(), any())

    val cp = classpath(client)
    cp should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))
    cp should not contain (jar3.getPath())
    cp should contain (jar4.getPath())
    cp should contain (buildPath(single.getAbsolutePath(), "*"))
  }

  test("distribute jars archive") {
    val temp = Utils.createTempDir()
    val archive = TestUtils.createJarWithFiles(Map(), temp)

    val sparkConf = new SparkConf().set(SPARK_ARCHIVE, archive.getPath())
    val client = createClient(sparkConf)
    client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)

    verify(client).copyFileToRemote(any(classOf[Path]), meq(new Path(archive.toURI())), anyShort(),
      anyBoolean(), any())
    classpath(client) should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))

    sparkConf.set(SPARK_ARCHIVE, LOCAL_SCHEME + ":" + archive.getPath())
    intercept[IllegalArgumentException] {
      client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)
    }
  }

  test("distribute archive multiple times") {
    val libs = Utils.createTempDir()
    // Create jars dir and RELEASE file to avoid IllegalStateException.
    val jarsDir = new File(libs, "jars")
    assert(jarsDir.mkdir())
    new FileOutputStream(new File(libs, "RELEASE")).close()

    val userLib1 = Utils.createTempDir()
    val testJar = TestUtils.createJarWithFiles(Map(), userLib1)

    // Case 1:  FILES_TO_DISTRIBUTE and ARCHIVES_TO_DISTRIBUTE can't have duplicate files
    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val client = createClient(sparkConf)
    val tempDir = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)
    }

    // Case 2: FILES_TO_DISTRIBUTE can't have duplicate files.
    val sparkConfFiles = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath, testJar.getPath))

    val clientFiles = createClient(sparkConfFiles)
    val tempDirForFiles = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      clientFiles.prepareLocalResources(new Path(tempDirForFiles.getAbsolutePath()), Nil)
    }

    // Case 3: ARCHIVES_TO_DISTRIBUTE can't have duplicate files.
    val sparkConfArchives = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath, testJar.getPath))

    val clientArchives = createClient(sparkConfArchives)
    val tempDirForArchives = Utils.createTempDir()
    intercept[IllegalArgumentException] {
      clientArchives.prepareLocalResources(new Path(tempDirForArchives.getAbsolutePath()), Nil)
    }

    // Case 4: FILES_TO_DISTRIBUTE can have unique file.
    val sparkConfFilesUniq = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(FILES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val clientFilesUniq = createClient(sparkConfFilesUniq)
    val tempDirForFilesUniq = Utils.createTempDir()
    clientFilesUniq.prepareLocalResources(new Path(tempDirForFilesUniq.getAbsolutePath()), Nil)

    // Case 5: ARCHIVES_TO_DISTRIBUTE can have unique file.
    val sparkConfArchivesUniq = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(ARCHIVES_TO_DISTRIBUTE, Seq(testJar.getPath))

    val clientArchivesUniq = createClient(sparkConfArchivesUniq)
    val tempDirArchivesUniq = Utils.createTempDir()
    clientArchivesUniq.prepareLocalResources(new Path(tempDirArchivesUniq.getAbsolutePath()), Nil)

  }

  test("distribute local spark jars") {
    val temp = Utils.createTempDir()
    val jarsDir = new File(temp, "jars")
    assert(jarsDir.mkdir())
    val jar = TestUtils.createJarWithFiles(Map(), jarsDir)
    new FileOutputStream(new File(temp, "RELEASE")).close()

    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> temp.getAbsolutePath()))
    val client = createClient(sparkConf)
    client.prepareLocalResources(new Path(temp.getAbsolutePath()), Nil)
    classpath(client) should contain (buildPath(PWD, LOCALIZED_LIB_DIR, "*"))
  }

  test("ignore same name jars") {
    val libs = Utils.createTempDir()
    val jarsDir = new File(libs, "jars")
    assert(jarsDir.mkdir())
    new FileOutputStream(new File(libs, "RELEASE")).close()
    val userLib1 = Utils.createTempDir()
    val userLib2 = Utils.createTempDir()

    val jar1 = TestUtils.createJarWithFiles(Map(), jarsDir)
    val jar2 = TestUtils.createJarWithFiles(Map(), userLib1)
    // Copy jar2 to jar3 with same name
    val jar3 = {
      val target = new File(userLib2, new File(jar2.toURI).getName)
      val input = new FileInputStream(jar2.getPath)
      val output = new FileOutputStream(target)
      Utils.copyStream(input, output, closeStreams = true)
      target.toURI.toURL
    }

    val sparkConf = new SparkConfWithEnv(Map("SPARK_HOME" -> libs.getAbsolutePath))
      .set(JARS_TO_DISTRIBUTE, Seq(jar2.getPath, jar3.getPath))

    val client = createClient(sparkConf)
    val tempDir = Utils.createTempDir()
    client.prepareLocalResources(new Path(tempDir.getAbsolutePath()), Nil)

    // Only jar2 will be added to SECONDARY_JARS, jar3 which has the same name with jar2 will be
    // ignored.
    sparkConf.get(SECONDARY_JARS) should be (Some(Seq(new File(jar2.toURI).getName)))
  }

  object Fixtures {

    val knownDefYarnAppCP: Seq[String] =
      getFieldValue[Array[String], Seq[String]](classOf[YarnConfiguration],
                                                "DEFAULT_YARN_APPLICATION_CLASSPATH",
                                                Seq[String]())(a => a.toSeq)


    val knownDefMRAppCP: Seq[String] =
      getFieldValue2[String, Array[String], Seq[String]](
        classOf[MRJobConfig],
        "DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH",
        Seq[String]())(a => a.split(","))(a => a.toSeq)

    val knownYARNAppCP = Some(Seq("/known/yarn/path"))

    val knownMRAppCP = Some(Seq("/known/mr/path"))

    val mapMRAppConf =
      Map("mapreduce.application.classpath" -> knownMRAppCP.map(_.mkString(":")).get)

    val mapYARNAppConf =
      Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> knownYARNAppCP.map(_.mkString(":")).get)

    val mapAppConf = mapYARNAppConf ++ mapMRAppConf
  }

  def withAppConf(m: Map[String, String] = Map())(testCode: (Configuration) => Any) {
    val conf = new Configuration
    m.foreach { case (k, v) => conf.set(k, v, "ClientSpec") }
    testCode(conf)
  }

  def newEnv: MutableHashMap[String, String] = MutableHashMap[String, String]()

  def classpath(env: MutableHashMap[String, String]): Array[String] =
    env(Environment.CLASSPATH.name).split(":|;|<CPS>")

  def flatten(a: Option[Seq[String]], b: Option[Seq[String]]): Array[String] =
    (a ++ b).flatten.toArray

  def getFieldValue[A, B](clazz: Class[_], field: String, defaults: => B)(mapTo: A => B): B = {
    Try(clazz.getField(field))
      .map(_.get(null).asInstanceOf[A])
      .toOption
      .map(mapTo)
      .getOrElse(defaults)
  }

  def getFieldValue2[A: ClassTag, A1: ClassTag, B](
        clazz: Class[_],
        field: String,
        defaults: => B)(mapTo: A => B)(mapTo1: A1 => B): B = {
    Try(clazz.getField(field)).map(_.get(null)).map {
      case v: A => mapTo(v)
      case v1: A1 => mapTo1(v1)
      case _ => defaults
    }.toOption.getOrElse(defaults)
  }

  private def createClient(
      sparkConf: SparkConf,
      conf: Configuration = new Configuration(),
      args: Array[String] = Array()): Client = {
    val clientArgs = new ClientArguments(args)
    spy(new Client(clientArgs, conf, sparkConf))
  }

  private def classpath(client: Client): Array[String] = {
    val env = new MutableHashMap[String, String]()
    populateClasspath(null, client.hadoopConf, client.sparkConf, env)
    classpath(env)
  }

}
