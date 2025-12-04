-- Create GTEx v8 tables
CREATE
OR REPLACE TABLE GTEx_v8_study AS (
    SELECT
        *
    FROM
        (
            SELECT
                *
            FROM
                read_parquet('eqtl-catalogue/study/*.parquet')
        )
    WHERE
        projectId = 'GTEx'
);

CREATE
OR REPLACE TABLE GTEx_v8_credible_set AS
SELECT
    *
FROM
    (
        SELECT
            *
        FROM
            read_parquet('eqtl-catalogue/credible_set/*.parquet')
    )
WHERE
    studyId IN (
        SELECT
            studyId
        FROM
            GTEx_v8_study
    );

-- Create GTEx v10 + majiq tables
CREATE
OR REPLACE TABLE GTEx_v10_study AS (
    SELECT
        *
    FROM
        read_parquet('gtex-v10-full_majiq/study_index/*/*.parquet')
);

CREATE
OR REPLACE TABLE GTEx_v10_credible_set AS (
    SELECT
        *
    FROM
        read_parquet('gtex-v10-full_majiq/study_locus/*/*.parquet')
);

-- Create stats views
CREATE
OR REPLACE VIEW gtexv10_study_comp AS
SELECT
    regexp_replace(studyId, '-v10', '') AS studyId,
    biosampleFromSourceId
FROM
    GTEx_v10_study;

CREATE
OR REPLACE VIEW gtex_credible_set AS (
    SELECT
        count_star() AS cnt,
        'v10' AS "release"
    FROM
        GTEx_v10_credible_set
)
UNION
BY NAME (
    SELECT
        count_star() AS cnt,
        'v8' AS "release"
    FROM
        GTEx_v8_credible_set
);

CREATE
OR REPLACE VIEW gtex_study AS (
    SELECT
        count_star() AS cnt,
        'v10' AS "release"
    FROM
        GTEx_v10_study
)
UNION
BY NAME (
    SELECT
        count_star() AS cnt,
        'v8' AS "release"
    FROM
        GTEx_v8_study
);

CREATE
OR REPLACE VIEW gtex_stats AS
SELECT
    "release",
    dataset,
    cnt,
    FORMAT('{:,f}', cnt) AS number,
    (
        (
            '+' || round(
                (
                    (
                        (cnt * 100) / min(cnt) OVER (PARTITION BY dataset)
                    ) - 100
                ),
                2
            )
        ) || '%'
    ) AS surplus
FROM
    (
        (
            SELECT
                *,
                'study' AS dataset
            FROM
                gtex_study
        )
        UNION
        BY NAME (
            SELECT
                *,
                'credible_set' AS dataset
            FROM
                gtex_credible_set
        )
    )
ORDER BY
    "release" DESC,
    dataset ASC;

-- Create sample size views
CREATE
OR REPLACE VIEW gtex_v8_sample_size AS
SELECT
    min,
    max,
    approx_unique,
    avg,
    std,
    q25,
    q50,
    q75,
    count,
    'v8' AS "release"
FROM
    (
        SUMMARIZE (
            SELECT
                nSamples
            FROM
                GTEx_v8_study
        )
    );

CREATE
OR REPLACE VIEW gtex_v10_sample_size AS
SELECT
    min,
    max,
    approx_unique,
    avg,
    std,
    q25,
    q50,
    q75,
    count,
    'v10' AS "release"
FROM
    (
        SUMMARIZE (
            SELECT
                nSamples
            FROM
                GTEx_v10_study
        )
    );

CREATE
OR REPLACE VIEW gtex_sample_size AS
SELECT
    "release",
    avg,
    std,
    min,
    max,
    q25,
    q50,
    q75,
    count
FROM
    (
        (
            SELECT
                *
            FROM
                gtex_v8_sample_size
        )
        UNION
        BY NAME (
            SELECT
                *
            FROM
                gtex_v10_sample_size
        )
    );

SELECT
    *
FROM
    gtex_sample_size;

-- Create credible set size views
CREATE
OR REPLACE VIEW gtex_v10_cs_len AS
SELECT
    min,
    max,
    approx_unique,
    avg,
    std,
    q25,
    q50,
    q75,
    count,
    'v10' AS "release"
FROM
    (
        SUMMARIZE (
            SELECT
                len(locus)
            FROM
                GTEx_v10_credible_set
        )
    );

CREATE
OR REPLACE VIEW gtex_v8_cs_len AS
SELECT
    min,
    max,
    approx_unique,
    avg,
    std,
    q25,
    q50,
    q75,
    count,
    'v8' AS "release"
FROM
    (
        SUMMARIZE (
            SELECT
                len(locus)
            FROM
                GTEx_v8_credible_set
        )
    );

CREATE
OR REPLACE VIEW gtex_cs_len AS
SELECT
    "release",
    avg,
    std,
    min,
    max,
    q25,
    q50,
    q75,
    count
FROM
    (
        (
            SELECT
                *
            FROM
                gtex_v8_cs_len
        )
        UNION
        BY NAME (
            SELECT
                *
            FROM
                gtex_v10_cs_len
        )
    );

SELECT
    *
FROM
    gtex_cs_len;

-- Create variant views
CREATE
OR REPLACE VIEW locus_v8 AS
SELECT
    locus.variantId,
    'v8' AS "release"
FROM
    (
        SELECT
            unnest(locus) AS locus
        FROM
            GTEx_v8_credible_set
    );

CREATE
OR REPLACE VIEW locus_v10 AS
SELECT
    locus.variantId,
    'v10' AS "release"
FROM
    (
        SELECT
            unnest(locus) AS locus
        FROM
            GTEx_v10_credible_set
    );

CREATE
OR REPLACE VIEW gtex_variant AS (
    SELECT
        *
    FROM
        locus_v8
)
UNION
BY NAME (
    SELECT
        *
    FROM
        locus_v10
);

CREATE
OR REPLACE VIEW gtex_variant_count AS (
    SELECT
        release,
        FORMAT('{:,f}', cnt) AS number,
        cnt,
        (
            (
                '+' || round((((cnt * 100) / min(cnt) OVER()) - 100), 2)
            ) || '%'
        ) AS surplus
    FROM
        (
            SELECT
                release,
                count(variantId) as cnt
            FROM
                gtex_variant
            GROUP BY
                release
        )
);

CREATE
OR REPLACE VIEW gtex_variant_distinct_count AS (
    SELECT
        release,
        FORMAT('{:,f}', cnt) AS number,
        cnt,
        (
            (
                '+' || round((((cnt * 100) / min(cnt) OVER()) - 100), 2)
            ) || '%'
        ) AS surplus
    FROM
        (
            SELECT
                release,
                count(variantId) as cnt
            FROM
                (
                    SELECT
                        DISTINCT variantId,
                        release
                    FROM
                        gtex_variant
                )
            GROUP BY
                release
        )
);

CREATE
OR REPLACE VIEW gtex_variant_overlap AS (
    SELECT
        count(variantId) as cnt,
        releases
    FROM
        (
            SELECT
                variantId,
                list_sort(list_distinct(list(release))) as releases
            FROM
                gtex_variant
            GROUP BY
                variantId
        )
    GROUP BY
        releases
);

CREATE
OR REPLACE VIEW gtex_variant_distinct_overlap AS (
    SELECT
        count(variantId) as cnt,
        releases
    FROM
        (
            SELECT
                variantId,
                list_sort(list_distinct(list(release))) as releases
            FROM
                (
                    SELECT
                        DISTINCT variantId,
                        release
                    FROM
                        gtex_variant
                )
            GROUP BY
                variantId
        )
    GROUP BY
        releases
);