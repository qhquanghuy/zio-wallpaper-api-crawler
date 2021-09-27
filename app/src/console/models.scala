package console

import scala.util.Try

import io.circe.Decoder
import io.circe.generic.auto._

import reactivemongo.api.bson._

final case class ApiResponse[A](count: Int, items: List[A])
final case class Category(id: Int, title: String)



final case class DeviceToken(
  id: BSONObjectID,
  deviceToken: String,
  appId: String,
  stale: Option[Boolean] = None
)

object DeviceToken extends {

  implicit object DeviceTokenReader extends BSONDocumentReader[DeviceToken] {
    def readDocument(doc: BSONDocument) = for {
      id <- doc.getAsTry[BSONObjectID]("_id")
      deviceToken <- doc.getAsTry[String]("deviceToken")
      appId <- doc.getAsTry[String]("appId")
    } yield DeviceToken(id, deviceToken, appId, doc.getAsOpt[Boolean]("stale"))
  }
}