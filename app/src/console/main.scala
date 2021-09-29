package console

import scala.util.Try
import scala.concurrent.duration.{Duration => ScDuration, _}
import scala.concurrent.Future
import java.time.temporal.ChronoUnit
import java.io.File

import zio._
import zio.console._
import zio.clock._
import zio.stream.{ZStream, ZSink}
import zio.config.magnolia.DeriveConfigDescriptor
import zio.config.ZConfig
import zio.duration.Duration
import zio.logging._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig

import sttp.client3.SttpBackend
import sttp.capabilities.zio._
import sttp.capabilities._
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend

import io.circe._
import io.circe.syntax._
import io.circe.optics.JsonPath._
import io.circe.bson._
import io.circe.parser._
import io.circe.generic.auto._

import reactivemongo.api._
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection._

import aliases._




object aliases {
  type SttpClient = Has[SttpBackend[Task, ZioStreams with WebSockets]]
  type MongoConf = Has[MongoConfig]
}

object constants {
  val limit = 200
}


object stream {


  def previewSmall() = {
    val widthIt = Iterator.from(600, 100).takeWhile(_ <= 4000)
    val widthHeightIt = widthIt.flatMap(width => Iterator.from(width + 200, 100).takeWhile(_ <= 4000).map(width -> _))
    ZStream.fromIterator(widthHeightIt)
      .mapMPar(2) {
        case (width, height) => api.images(1, 0, width, height)
      }
      .mapConcat { res =>
        val base = root.each.variations.preview_small.resolution
        val resolutions = base.json.getAll(res.items.asJson)
        val widthLens = root.width.int
        val heightLens = root.height.int
        resolutions.flatMap { resolution =>
          for {
            w <- widthLens.getOption(resolution)
            h <- heightLens.getOption(resolution)
          } yield w -> h
        }
        .toSet
      }
  }

  def dimensions = {
    val xs = Seq(
      (240,506),
      (938,1668),
      (1336,1336),
      (480,854),
      (256,456),
      (360,720),
      (600,1024),
      (1080,1920),
      (768,1024),
      (1200,1920),
      (480,986),
      (312,556),
      (200,342),
      (768,1336),
      (600,854),
      (480,800),
      (800,1420),
      (266,473),
      (240,320),
      (480,720),
      (240,426),
      (480,960),
      (854,1139),
      (720,1280),
      (360,780),
      (695,927),
      (640,854),
      (400,640),
      (360,640),
      (480,1040),
      (360,760),
      (535,1157),
      (320,480),
      (469,1015),
      (266,426),
      (534,854),
      (540,960),
      (800,1280),
      (450,800)
    )
    xs
  }

  def totalPage(totalItems: Int) = Math.ceil(totalItems.toDouble / constants.limit).toLong

  def images(errorQ: Queue[Throwable]) = {

    ZStream.fromIterable(dimensions)
      .map {
        case (w, h) => w * 3 -> h * 3
      }
      .flatMap {
        case (w, h) => ZStream.fromEffect(offsetStream(api.images(1, 0, w, h).map(_.count))).mapConcat(identity).map(_ -> (w, h))
      }
      .chunkN(1).throttleShape(1, Duration.fromScala(250.millis))(_ => 1)
      .mapMParUnordered(5) {
        case (offset, (w, h)) => api.images(constants.limit, offset, w, h).either
      }
      .tap {
        case Left(throwable) =>
          errorQ.offer(throwable)
        case _ => ZIO.unit
      }
      .collectRight
      .map(_.items)
      .mapConcat(identity)
  }

  def offsetStream[R, E](eff: ZIO[R, E, Int]) = {
    eff.map(totalPage)
      .map(pages => Range(0, pages.toInt))
      .map(_.map(_ * constants.limit))

  }

  def categories(errorQ: Queue[Throwable]) = {

    val eff = offsetStream(api.categories(1, 0, 480, 640).map(_.count))

    ZStream.fromEffect(eff)
      .mapConcat(identity)
      .mapMParUnordered(2)(api.categories(constants.limit, _, 480, 640).either)
      .tap {
        case Left(throwable) =>
          errorQ.offer(throwable)
        case _ => ZIO.unit
      }
      .collectRight
      .mapConcat(_.items)
  }

  def deviceTokens() = {
    val initState: Option[BSONObjectID] = None
    val pageSize = 1000
    ZStream.unfoldM(initState) { maybeLastId =>
      db.fetch(maybeLastId, pageSize)
        .map { xs =>
          if (xs.isEmpty) None else Some(xs -> xs.lastOption.map(_.id))
        }
    }
  }

}


object mongo {
  import reactivemongo.api.{ AsyncDriver, MongoConnection }

  lazy val layer = {
    ZLayer.fromAcquireRelease(
      for {
        config <- ZIO.access[MongoConf](_.get)
        driver = new AsyncDriver
        conStr = s"mongodb://${config.user}:${config.pwd}@${config.host}:${config.port}/?readPreference=primary&ssl=false&authSource=${config.database}"
        conn <- ZIO.fromFuture(implicit ec => driver.connect(conStr))
      } yield conn
    )(driver => ZIO.fromFuture(implicit ec => driver.close()(1.minute)).ignore)
  }
}

object db {


  import reactivemongo.api.bson._
  import reactivemongo.api.bson.collection._


  trait Service {
    def upsertCategories(categories: Seq[Category]): ZIO[Logging, Throwable, Unit]
    def upsertImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
    def upsertImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
    def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int): ZIO[Logging, Throwable, Array[DeviceToken]]
  }

  object Service {

    lazy val live = {
      ZLayer.fromService { (pair: (DB, MongoConfig)) =>
        val (db, conf) = pair
        lazy val deviceTokens = db.collection("deviceTokens")
        new Service {

          private def json2BSONDoc(json: Json) = ZIO.fromEither(jsonToBson(json))
            .collect(new IllegalArgumentException("Inserting Json  must be an Object")) { case doc: BSONDocument => doc }


          private def upsert(collectionName: String, xs: Seq[(Json, BSONValue)]) = {
            val collection = db.collection(collectionName)
            val (notJsObjs, jsObjs) = xs.map {
              case (json, id) => jsonToBson(json) -> id
            }
            .partition(_._1.isLeft)

            val updateBuilder = collection.update
            val updatesF = jsObjs.collect {
              case (Right(bdoc), id) => updateBuilder.element(
                q = BSONDocument("id" -> id),
                u = BSONDocument("$set" -> bdoc),
                upsert = true
              )
            }

            val eff = ZIO.fromFuture { implicit ec =>
              Future.sequence(updatesF).flatMap { updates => updateBuilder.many(updates) }
            }

            val id2InsertHash = jsObjs.map(_._2).hashCode()

            for {
              _ <- if (notJsObjs.nonEmpty) log.error(s"Not Json Object: ${notJsObjs.map(_._2)}") else ZIO.unit
              _ <- log.info(s"Loading ids: ${id2InsertHash}")
              result <- eff
              _ <- log.info(s"Done Loading " +
                s"id: ${id2InsertHash} " +
                s"ok = ${result.ok} " +
                s"n = ${result.n} " +
                s"nModified = ${result.nModified} " +
                s"writeErrors = ${result.writeErrors} " +
                s"writeConcernError = ${result.writeConcernError} " +
                s"code = ${result.code} " +
                s"errmsg = ${result.errmsg} " +
                s"totalN = ${result.totalN}}"
              )
            } yield ()
          }

          override def upsertCategories(categories: Seq[Category]) = {
            upsert("categories", categories.map(category => category.asJson -> BSONInteger(category.id)))
          }

          override def upsertImages(xs: Seq[Json]): ZIO[Logging,Throwable,Unit] = {
            val images2Upsert = xs.flatMap { js =>
              root.id.int.getOption(js).map(id => js -> BSONInteger(id))
            }
            upsert("images", images2Upsert)
          }
          override def upsertImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit] = {
            val variations2Upsert = xs.flatMap { js =>
              root.id.string.getOption(js).map(id => js -> BSONString(id))
            }
            upsert("variations", variations2Upsert)
          }


          override def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int): ZIO[Logging, Throwable, Array[DeviceToken]] = {
            ZIO.fromFuture { implicit ec =>
              val filter = maybeLastId
                .map(lastId =>
                  BSONDocument("_id" -> BSONDocument("$lt" -> lastId))
                )
                .getOrElse(BSONDocument())

              deviceTokens.find(filter)
                .sort(BSONDocument("_id" -> -1))
                .cursor[DeviceToken]()
                .collect(pageSize, Cursor.ContOnError { (a: Array[DeviceToken], throwable) =>
                  throwable.printStackTrace()
                })
            }
          }

        }
      }
    }
  }

  def upsertCategories(categories: Seq[Category]) = {
     ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.upsertCategories(categories))
  }

  def upsertImages(xs: Seq[Json]) = {
    ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.upsertImages(xs))
  }
  def upsertImageVariations(xs: Seq[Json]) = {
    ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.upsertImageVariations(xs))
  }

  def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int) = {
     ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.fetch(maybeLastId, pageSize))
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

object logic {
  def separateImageDataAndVariation(js: Json) = {
    for {
      obj <- js.asObject
      id <- obj("id")
      variations <- obj("variations").flatMap(_.asObject)
      variationsJson = variations.asJson
      adaptedUrl <- root.adapted.url.string.getOption(variationsJson)
      adapted_landscapeUrl <- root.adapted_landscape.url.string.getOption(variationsJson)
      originalUrl <- root.original.url.string.getOption(variationsJson)
      preview_smallUrl <- root.preview_small.url.string.getOption(variationsJson)
      hash = util.md5HashString(adaptedUrl + adapted_landscapeUrl + originalUrl + preview_smallUrl)
    } yield obj.remove("variations") -> variations.add("image_id", id).add("id", hash.asJson)
  }
}

object main extends App {


  def sendFCM(fcmConfig: FCMConfig) = {
    val keys = Map(
      "Meditation Music" -> fcmConfig.meditationMusicKey,
      "Relax Sound" -> fcmConfig.relaxSoundKey
    )
    stream.deviceTokens()
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
    stream
    .previewSmall()
    .run(ZSink.collectAllToSet)
  }

  def imageProg(errorQ: Queue[Throwable]) = {

    stream.images(errorQ)
      .mapM(js => ZIO.fromOption(logic.separateImageDataAndVariation(js)))
      .grouped(1024)
      .mapMParUnordered(8) { chunk =>
        (db.upsertImages(chunk.map(_._1.asJson)) <*> db.upsertImageVariations(chunk.map(_._2.asJson))).either
      }
      .tap {
        case Left(throwable) =>
          errorQ.offer(throwable)
        case _ => ZIO.unit
      }
      .collectRight
      .run(ZSink.foldLeft(0)((acc, _) => acc + 1))
  }


  def categoryProg(errorQ: Queue[Throwable]) = {
    stream.categories(errorQ)
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
    } yield categorySuccesses
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