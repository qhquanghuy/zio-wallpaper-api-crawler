package console

import scala.concurrent.duration._
import java.time.temporal.ChronoUnit
import java.io.File

import zio._
import zio.clock._
import zio.stream.{ZStream, ZSink}
import zio.config.magnolia.DeriveConfigDescriptor
import zio.logging._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig

import sttp.client3.SttpBackend
import sttp.capabilities.zio._
import sttp.capabilities._
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend

import io.circe._
import io.circe.optics.JsonPath._

import reactivemongo.api._

import aliases._
import transform._
import load._



object aliases {
  type SttpClient = Has[SttpBackend[Task, ZioStreams with WebSockets]]
  type MongoConf = Has[MongoConfig]
}

object constants {
  val limit = 200
}




object mongo {
  import reactivemongo.api.AsyncDriver

  lazy val layer = {
    ZLayer.fromAcquireRelease(
      for {
        config <- ZIO.access[MongoConf](_.get)
        driver = new AsyncDriver
        conStr = s"mongodb://${config.user}:${config.pwd}@${config.host}:${config.port}/${config.database}?authSource=admin"
        conn <- ZIO.fromFuture(implicit ec => driver.connect(conStr))
      } yield conn
    )(driver => ZIO.fromFuture(implicit ec => driver.close()(1.minute)).ignore)
  }
}


object util {
  def md5HashString(s: String): String = {
    import java.security.MessageDigest
    import java.math.BigInteger
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1,digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }
}


object main extends App {


  def sendFCM(fcmConfig: FCMConfig) = {
    val keys = Map(
      "Meditation Music" -> fcmConfig.meditationMusicKey,
      "Relax Sound" -> fcmConfig.relaxSoundKey
    )
    extract.stream.deviceTokens()
      .flatMap { deviceTokens =>
        val groups = deviceTokens.groupBy(_.appId)
        ZStream.fromIterable(groups)
      }
      .mapM {
        case (appId, deviceTokens) => api.sendFCM(keys(appId), appId, deviceTokens.map(_.deviceToken)).either
      }
      .tap {
        case Right(json) => log.info(s"main::sendFCM ${json.toString()}")
        case Left(throwable) => log.error(s"main::sendFCM", Cause.Die(throwable))
      }
      .run(
        ZSink.foldLeft(0 -> 0) {
          case ((totalSuccess, totalFailure), Right(json)) =>
            val success = root.success.int.getOption(json).map(_ + totalSuccess).getOrElse(totalSuccess)
            val failure = root.failure.int.getOption(json).map(_ + totalFailure).getOrElse(totalFailure)
            success -> failure
          case (acc, Left(throwable)) => acc
        }
      )
  }


  def getDimension = {
    extract.stream
      .previewSmall()
      .run(ZSink.collectAllToSet)
  }

  def dataProg[R1, R2, E, A, B](
    mkExtractStream: (Queue[E]) => ZStream[R1, E, A],
    transform: (A) => Option[(B, B)],
    load: Chunk[(B, B)] => ZIO[R2, E, Unit],
    errorQ: Queue[E]
  ) = {
    mkExtractStream(errorQ)
      .mapM(x => ZIO.fromOption(transform(x)))
      .grouped(512)
      .mapMParUnordered(6)(x => load(x).either)
      .tap {
        case Left(throwable) =>
          errorQ.offer(throwable)
        case _ => ZIO.unit
      }
      .collectRight
      .run(ZSink.foldLeft(0)((acc, _) => acc + 1))
  }

  def imageProg(errorQ: Queue[Throwable]) = {
    dataProg(extract.stream.images, logic.separateImageDataAndVariation, { (chunk: Chunk[(Json, Json)]) =>
      db.upsertImages(chunk.map(_._1)) *> db.upsertImageVariations(chunk.map(_._2))
    }, errorQ)

  }

  def doubleImageProg(errorQ: Queue[Throwable]) = {
    dataProg(extract.stream.doubleImages, logic.separateDoubleImageDataAndVariation, { (chunk: Chunk[(Json, Json)]) =>
      db.upsertDoubleImages(chunk.map(_._1)) *> db.upsertDoubleImageVariations(chunk.map(_._2))
    }, errorQ)

  }

  def liveImageProg(errorQ: Queue[Throwable]) = {
    dataProg(extract.stream.liveImages, logic.separateLiveImageDataAndVariation, { (chunk: Chunk[(Json, Json)]) =>
      db.upsertLiveImages(chunk.map(_._1)) *> db.upsertLiveImageVariations(chunk.map(_._2))
    }, errorQ)

  }


  def categoryProg(errorQ: Queue[Throwable]) = {
    extract.stream.categories(errorQ)
      .grouped(128)
      .mapMParUnordered(32)(xs => db.upsertCategories(xs).either)
      .tap {
        case Left(throwable) =>
          errorQ.offer(throwable)
        case _ => ZIO.unit
      }
      .collectRight
      .run(ZSink.foldLeft(0)((acc, _) => acc + 1))
  }

  def mainProg(errorQ: Queue[Throwable]) = {
    for {
      categorySuccesses <- categoryProg(errorQ)
      imagesSuccesses <- imageProg(errorQ)
      // doubleImagesSuccesses <- doubleImageProg(errorQ)
      // liveImagesSuccesses <- liveImageProg(errorQ)
    } yield imagesSuccesses
  }


  def program = {
    for {
      queue <- ZQueue.unbounded[Throwable]
      failuresFiber <- ZStream.fromQueue(queue)
        .tap(log.throwable("categoryProg", _))
        .run(ZSink.foldLeft(0)((acc, _) => acc + 1))
        .fork

      mainFiber <- mainProg(queue).tap(_ => queue.shutdown).fork
      failures <- failuresFiber.join
      categorySuccesses <- mainFiber.join

    } yield failures -> categorySuccesses

  }


  def run(args: List[String]): URIO[ZEnv, ExitCode] = {

    val appConfigLayer = TypesafeConfig.fromHoconFile(new File("./.env.conf"), DeriveConfigDescriptor.descriptor[AppConfig])

    val logLayer = Logging.console()

    val apiConfigLayer = appConfigLayer.narrow(_.api)
    val httpClientLayer = AsyncHttpClientZioBackend.layer() ++ apiConfigLayer

    val fcmConfigLayer = appConfigLayer.narrow(_.fcm)
    val configLayer = appConfigLayer.narrow(_.db)
    val facebookConfigLayer = appConfigLayer.narrow(_.fb)

    val mongoLayer = configLayer >>> mongo.layer

    val dbLayer = ZLayer.fromServiceM { (conn: MongoConnection) =>
      for {
        conf <- ZIO.access[MongoConf](_.get)
        db <- ZIO.fromFuture(implicit ec => conn.database(conf.database))
      } yield db -> conf
    }

    val dbServiceLayer = (mongoLayer ++ configLayer) >>> dbLayer >>> db.Service.live

    val layer = httpClientLayer ++ dbServiceLayer ++ logLayer ++ facebookConfigLayer ++ fcmConfigLayer

    (for {
      start <- instant
      _ <- log.info(s"Starting...")
      (failures, successes) <- program
      endTime <- instant
      message = s"""
                   |Total run times: ${start.until(endTime, ChronoUnit.MINUTES)}m ${start.until(endTime, ChronoUnit.SECONDS)}s
                   | successes: $successes, failures: $failures
                   |""".stripMargin
    } yield message)
    .flatMap(message => {
      log.info(message) *> api.sendFacebookMessage(message)
    })
    .provideCustomLayer(layer)
    .exitCode
  }
}