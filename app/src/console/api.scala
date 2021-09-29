package console

import zio._
import zio.config._
import zio.logging._

import sttp.client3.SttpBackend
import sttp.capabilities.zio._
import sttp.capabilities._
import sttp.client3.circe._
import sttp.client3._
import sttp.client3.asynchttpclient.zio._
import sttp.model.Uri

import io.circe.generic.auto._
import io.circe.Json
import io.circe.syntax._


object api {

  val baseReq = basicRequest

  def run[L <: Throwable, R](req: Request[Either[L, R], Effect[Task] with ZioStreams with WebSockets]) = {
    for {
      _ <- log.info(s"${req.method} ${req.uri}")
      rawRes <- send(req)
      _ <- log.info(s"${req.method} ${req.uri} --- ${rawRes.code}")
      res <- ZIO.fromEither(rawRes.body)
    } yield res

  }


  def mkUri(path: String) = ZIO.access[Has[ApiConfig]](_.get).map(conf => Uri.unsafeParse(s"${conf.host}$path"))

  def baseUri(path: String, limit: Int, offset: Int, width: Int, height: Int) = mkUri(path).map { uri =>
    uri.addParam("limit", limit.toString())
      .addParam("offset", offset.toString())
      .addParam("screen[width]", width.toString())
      .addParam("screen[height]", height.toString())
  }


  def images(limit: Int, offset: Int, width: Int, height: Int) = {
    for {
      uri <- baseUri("/images", limit, offset, width, height)
      req = baseReq.get(uri).response(asJson[ApiResponse[Json]])
      res <- run(req)
    } yield res
  }

  def categories(limit: Int, offset: Int, width: Int, height: Int) = {
    for {
      uri <- baseUri("/categories", limit, offset, width, height)
      req = baseReq.get(uri).response(asJson[ApiResponse[Category]])
      res <- run(req)
    } yield res
  }

  def liveImages(limit: Int, offset: Int, width: Int, height: Int) = {
    for {
      uri <- baseUri("/live-images", limit, offset, width, height)
      req = baseReq.get(uri.addParam("content_type", "ios_live")).response(asJson[ApiResponse[Json]])
      res <- run(req)
    } yield res
  }

  def doubleImages(limit: Int, offset: Int, width: Int, height: Int) = {
    for {
      uri <- baseUri("/double-images", limit, offset, width, height)
      req = baseReq.get(uri).response(asJson[ApiResponse[Json]])
      res <- run(req)
    } yield res
  }

  def sendFacebookMessage(message: String) = {

    getConfig[FacebookConfig] >>= { config =>
      val req = basicRequest.post(uri"https://graph.facebook.com/v11.0/me/messages?access_token=${config.pageAccessToken}")
        .body(Json.obj(
          "recipient" -> Json.obj(
            "id" -> config.userPSID.asJson
          ),
          "message" -> Json.obj(
            "text" -> message.asJson
          )
        )
      )
      .response(asStringAlways)

      send(req)
    }
  }


  def sendFCM(key: String, appId: String, deviceTokens: Array[String]) = {
    val req = basicRequest.post(uri"https://fcm.googleapis.com/fcm/send")
      .header("Authorization", s"key=${key}")
      .body(
        Json.obj(
          "registration_ids" -> deviceTokens.asJson,
          "notification" -> Json.obj(
            "title" -> appId.asJson,
            "body" -> "There are several new content! Check it out".asJson
          )
        )
      )
      .response(asJson[Json])

    run(req)
  }
}