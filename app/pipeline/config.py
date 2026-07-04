FOLDER_IDS = {
    "movie": "3464134653584082023",
    "tv": "3465137076394001831",
    "adult": "3464134590896014943",
    "other": "3465205291639899794",
}

OPENLIST_PATHS = {
    "movie": "/115/电影",
    "tv": "/115/剧集",
    "adult": "/115/成人",
    "other": "/115/其他",
}

MSG_LIBRARY_ROOTS = {
    "movie": {
        "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
        "root_id": "0c1dda42-29ef-4069-b051-c9549a8d4440",
        "provider": "tmdb",
        "media_type": "movie",
    },
    "tv": {
        "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
        "root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
        "provider": "tmdb",
        "media_type": "tv",
    },
    "adult": {
        "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9",
        "root_id": "3fe479e8-4a96-4e61-9f69-fa802e448446",
        "provider": "adult",
        "media_type": "adult",
    },
    "other": {
        "library_id": "60067bc7-eb34-466c-8bf9-5654297a609f",
        "root_id": "1f889ec1-b34d-40b6-b3ca-f4372170a42b",
        "provider": "tmdb",
        "media_type": "movie",
    },
}


def category_to_folder_id(category):
    try:
        return FOLDER_IDS[category]
    except KeyError:
        raise ValueError("unsupported category: %s" % category)


def category_to_openlist_path(category):
    try:
        return OPENLIST_PATHS[category]
    except KeyError:
        raise ValueError("unsupported category: %s" % category)


def category_to_msg_library_root(category):
    try:
        return dict(MSG_LIBRARY_ROOTS[category])
    except KeyError:
        raise ValueError("unsupported category: %s" % category)
