package console

import scala.concurrent.Future

import zio._
import zio.logging._

import io.circe._
import io.circe.syntax._
import io.circe.optics.JsonPath._
import io.circe.bson._
import io.circe.generic.auto._


import reactivemongo.api._
import reactivemongo.api.bson._

object load {
  object db {

    trait Service {
      def upsertCategories(categories: Seq[Category]): ZIO[Logging, Throwable, Unit]
      def upsertImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
      def upsertImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
      def upsertDoubleImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
      def upsertDoubleImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
      def upsertLiveImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
      def upsertLiveImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit]
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

            override def upsertDoubleImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit] = {
              val images2Upsert = xs.flatMap { js =>
                root.id.int.getOption(js).map(id => js -> BSONInteger(id))
              }
              upsert("doubleImages", images2Upsert)
            }
            override def upsertDoubleImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit] = {
              val variations2Upsert = xs.flatMap { js =>
                root.id.string.getOption(js).map(id => js -> BSONString(id))
              }
              upsert("doubleImageVariations", variations2Upsert)
            }

            override def upsertLiveImages(xs: Seq[Json]): ZIO[Logging, Throwable, Unit] = {
              val images2Upsert = xs.flatMap { js =>
                root.id.int.getOption(js).map(id => js -> BSONInteger(id))
              }
              upsert("liveImages", images2Upsert)
            }
            override def upsertLiveImageVariations(xs: Seq[Json]): ZIO[Logging, Throwable, Unit] = {
              val variations2Upsert = xs.flatMap { js =>
                root.id.string.getOption(js).map(id => js -> BSONString(id))
              }
              upsert("liveImageVariations", variations2Upsert)
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

    def upsertDoubleImages(xs: Seq[Json]) = {
      ZIO.access[Has[db.Service]](_.get)
        .flatMap(_.upsertDoubleImages(xs))
    }
    def upsertDoubleImageVariations(xs: Seq[Json]) = {
      ZIO.access[Has[db.Service]](_.get)
        .flatMap(_.upsertDoubleImageVariations(xs))
    }

    def upsertLiveImages(xs: Seq[Json]) = {
      ZIO.access[Has[db.Service]](_.get)
        .flatMap(_.upsertLiveImages(xs))
    }
    def upsertLiveImageVariations(xs: Seq[Json]) = {
      ZIO.access[Has[db.Service]](_.get)
        .flatMap(_.upsertLiveImageVariations(xs))
    }

    def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int) = {
      ZIO.access[Has[db.Service]](_.get)
        .flatMap(_.fetch(maybeLastId, pageSize))
    }
  }

}
