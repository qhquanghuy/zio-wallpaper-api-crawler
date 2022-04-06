# Intro

Simple ETL using ZIO Stream, reactivemongo and scala build tool mill

Scrape data from API then load it to mongodb. Send notifications to facebook page when job is done

# APIs

1. /categories?limit=100&offset=0&screen[width]=100&screen[height]=200

```json
{
    "data": [
        {
            "title": "Music",
            "id": 46
        }
    ]
}
```
2. /images?limit=1&offset=0&screen[width]=1000&screen[height]=2000&category_id=48&sort=rating

```json
{
    "data": [
        {
            "author": "Author",
            "favorites": 5802,
            "downloads": 23156214,
            "content_type": "free",
            "user_id": 0,
            "tags": [
                "mask",
                "hood",
                "magic",
                "ball",
                "fire"
            ],
            "colors": [
                [
                    141,
                    111,
                    94
                ],
                [
                    67,
                    46,
                    40
                ],
                [
                    218,
                    205,
                    154
                ]
            ],
            "theme_color": [
                114,
                252,
                94
            ],
            "uploader_type": "wlc",
            "id": 132368,
            "rating": 32987,
            "cost": 0,
            "license": "CC0",
            "description": "mask, hood, magic, ball, fire",
            "for_adult_only": false,
            "variations": {
                "adapted": {
                    "resolution": {
                        "width": 1080,
                        "height": 2160
                    },
                    "size": 338057,
                    "url": "/image/single/132368_1080x2160.jpg"
                },
                "preview_small": {
                    "url": "/image/single/132368_360x720.jpg",
                    "size": 45239,
                    "resolution": {
                        "width": 360,
                        "height": 720
                    }
                },
                "original": {
                    "resolution": {
                        "width": 3332,
                        "height": 3710
                    },
                    "size": 2382555,
                    "url": "/image/single/132368_3332x3710.jpg"
                },
                "adapted_landscape": {
                    "url": "/image/single/132368_2160x2160.jpg",
                    "size": 703266,
                    "resolution": {
                        "width": 2160,
                        "height": 2160
                    }
                }
            },
            "nft_links": [],
            "source_link": "",
            "category_id": 48,
            "min_cost_ends_at": "2018-12-24T11:56:10Z",
            "uploaded_at": "2018-12-24T14:56:10+0300"
        }
    ]
}
```
