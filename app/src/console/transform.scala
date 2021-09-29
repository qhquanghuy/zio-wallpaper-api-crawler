package console


import io.circe._
import io.circe.syntax._
import io.circe.optics.JsonPath._



object transform {
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
      } yield obj.remove("variations").asJson -> variations.add("image_id", id).add("id", hash.asJson).asJson
    }

    def separateDoubleImageDataAndVariation(js: Json) = {
      for {
        obj <- js.asObject
        id <- obj("id")

        homeVariations <- obj("home_variations").flatMap(_.asObject)
        lockVariations <- obj("lock_variations").flatMap(_.asObject)

        homeVariationsJson = homeVariations.asJson
        lockVariationsJson = lockVariations.asJson

        homeAdaptedUrl <- root.adapted.url.string.getOption(homeVariationsJson)
        homePreview_smallUrl <- root.preview_small.url.string.getOption(homeVariationsJson)

        lockAdaptedUrl <- root.adapted.url.string.getOption(lockVariationsJson)
        lockPreview_smallUrl <- root.preview_small.url.string.getOption(lockVariationsJson)

        hash = util.md5HashString(homeAdaptedUrl + homePreview_smallUrl + lockAdaptedUrl + lockPreview_smallUrl)
      } yield obj.remove("home_variations").remove("lock_variations").asJson -> Json.obj(
        "double_image_id" -> id.asJson,
        "id" -> hash.asJson,
        "home_variations" -> homeVariationsJson,
        "lock_variations" -> lockVariationsJson
      )
    }


    def separateLiveImageDataAndVariation(js: Json) = {
      for {
        obj <- js.asObject
        id <- obj("id")

        imageVariations <- obj("image_variations").flatMap(_.asObject)
        videoVariations <- obj("video_variations").flatMap(_.asObject)

        imageVariationsJson = imageVariations.asJson
        videoVariationsJson = videoVariations.asJson

        imageAdaptedUrl <- root.adapted.url.string.getOption(imageVariationsJson)
        imagePreview_smallUrl <- root.preview_small.url.string.getOption(imageVariationsJson)

        videoAdaptedUrl <- root.adapted.url.string.getOption(videoVariationsJson)
        videoPreview_smallUrl <- root.preview_small.url.string.getOption(videoVariationsJson)

        hash = util.md5HashString(imageAdaptedUrl + imagePreview_smallUrl + videoAdaptedUrl + videoPreview_smallUrl)
      } yield obj.remove("image_variations").remove("video_variations").asJson -> Json.obj(
        "live_image_id" -> id.asJson,
        "id" -> hash.asJson,
        "image_variations" -> imageVariationsJson,
        "video_variations" -> videoVariationsJson
      )
    }
  }

}
