"""Schéma explicite du message TMDB déposé dans le Data Lake.

Déclarer le schéma plutôt que le laisser inférer répond à l'exigence de contrôle
des types, et évite surtout deux pièges classiques de l'inférence :

- un champ absent ou nul dans l'échantillon lu prend un type arbitraire, qui
  change d'une exécution à l'autre selon les films collectés ;
- l'inférence impose une passe de lecture supplémentaire sur tout le lac.

Les champs inconnus du schéma sont simplement ignorés à la lecture : le fichier
brut, lui, les conserve.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def _person_fields() -> list[StructField]:
    """Champs communs au casting et à l'équipe technique."""
    return [
        StructField("id", IntegerType()),
        StructField("credit_id", StringType()),
        StructField("name", StringType()),
        StructField("original_name", StringType()),
        StructField("gender", IntegerType()),
        StructField("known_for_department", StringType()),
        StructField("popularity", DoubleType()),
        StructField("profile_path", StringType()),
    ]


CAST_MEMBER = StructType(
    _person_fields()
    + [StructField("character", StringType()), StructField("order", IntegerType())]
)

CREW_MEMBER = StructType(
    _person_fields()
    + [StructField("department", StringType()), StructField("job", StringType())]
)

COLLECTION = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("poster_path", StringType()),
        StructField("backdrop_path", StringType()),
    ]
)

GENRE = StructType([StructField("id", IntegerType()), StructField("name", StringType())])

COMPANY = StructType(
    [
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("origin_country", StringType()),
        StructField("logo_path", StringType()),
    ]
)

COUNTRY = StructType(
    [StructField("iso_3166_1", StringType()), StructField("name", StringType())]
)

LANGUAGE = StructType(
    [
        StructField("iso_639_1", StringType()),
        StructField("name", StringType()),
        StructField("english_name", StringType()),
    ]
)

MOVIE_PAYLOAD = StructType(
    [
        StructField("id", IntegerType()),
        StructField("imdb_id", StringType()),
        StructField("title", StringType()),
        StructField("original_title", StringType()),
        StructField("original_language", StringType()),
        StructField("overview", StringType()),
        StructField("tagline", StringType()),
        StructField("status", StringType()),
        # Date laissée en chaîne : TMDB renvoie "" pour une date inconnue, ce qui
        # ferait échouer une lecture typée. La conversion est faite au nettoyage.
        StructField("release_date", StringType()),
        StructField("runtime", IntegerType()),
        StructField("budget", LongType()),
        StructField("revenue", LongType()),
        StructField("popularity", DoubleType()),
        StructField("vote_average", DoubleType()),
        StructField("vote_count", IntegerType()),
        StructField("adult", BooleanType()),
        StructField("homepage", StringType()),
        StructField("poster_path", StringType()),
        StructField("backdrop_path", StringType()),
        StructField("belongs_to_collection", COLLECTION),
        StructField("genres", ArrayType(GENRE)),
        StructField("production_companies", ArrayType(COMPANY)),
        StructField("production_countries", ArrayType(COUNTRY)),
        StructField("spoken_languages", ArrayType(LANGUAGE)),
        StructField(
            "credits",
            StructType(
                [
                    StructField("cast", ArrayType(CAST_MEMBER)),
                    StructField("crew", ArrayType(CREW_MEMBER)),
                ]
            ),
        ),
    ]
)

# Enveloppe écrite par le producer, autour du payload TMDB intact
ENVELOPE = StructType(
    [
        StructField("source", StringType()),
        StructField("endpoint", StringType()),
        StructField("movie_id", LongType()),
        StructField("fetched_at", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("payload", MOVIE_PAYLOAD),
    ]
)

# Datasets IMDb : tout est lu en chaîne puis converti explicitement, car la
# convention IMDb pour une valeur absente ("\N") n'est pas un null reconnu par
# l'inférence de type.
TITLE_RATINGS = StructType(
    [
        StructField("tconst", StringType()),
        StructField("averageRating", StringType()),
        StructField("numVotes", StringType()),
    ]
)

TITLE_BASICS = StructType(
    [
        StructField("tconst", StringType()),
        StructField("titleType", StringType()),
        StructField("primaryTitle", StringType()),
        StructField("originalTitle", StringType()),
        StructField("isAdult", StringType()),
        StructField("startYear", StringType()),
        StructField("endYear", StringType()),
        StructField("runtimeMinutes", StringType()),
        StructField("genres", StringType()),
    ]
)
