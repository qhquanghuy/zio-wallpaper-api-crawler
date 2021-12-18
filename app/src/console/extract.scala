package console

import scala.concurrent.duration._
import zio._
import zio.stream.ZStream
import zio.duration.Duration


import io.circe.syntax._
import io.circe.optics.JsonPath._

import reactivemongo.api.bson._

import load._

object extract {
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
      scala.util.Random.shuffle(xs)
    }

    def totalPage(totalItems: Int) = Math.ceil(totalItems.toDouble / constants.limit).toLong

    def mkDataStream[R, E, A](dataSource: (Int, Int, Int, Int) => ZIO[R, E, ApiResponse[A]], errorQ: Queue[E]) = {
      ZStream.fromIterable(dimensions)
        .map {
          case (w, h) => w * 3 -> h * 3
        }
        .flatMap {
          case (w, h) => ZStream.fromEffect(offsetStream(dataSource(1, 0, w, h).map(_.count))).mapConcat(identity).map(_ -> (w, h)).either
        }
        .tap {
          case Left(throwable) =>
            errorQ.offer(throwable)
          case _ => ZIO.unit
        }
        .collectRight
        .chunkN(1).throttleShape(1, Duration.fromScala(250.millis))(_ => 1)
        .mapMParUnordered(5) {
          case (offset, (w, h)) => dataSource(constants.limit, offset, w, h).either
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

    def images(errorQ: Queue[Throwable]) = {
      mkDataStream(api.images, errorQ)
    }

    def doubleImages(errorQ: Queue[Throwable]) = {
      mkDataStream(api.doubleImages, errorQ)
    }
    def liveImages(errorQ: Queue[Throwable]) = {
      mkDataStream(api.liveImages, errorQ)
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

}
