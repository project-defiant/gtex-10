library(arrow)
library(ggplot2)
library(dplyr)
library(readr)
library(tidyr)
library(duckdb)
library(dbplyr)
library(tibble)
library(ggpubr)
library(scales)

my_colors_p1 <- c("#2a5d34", "#d6a941")
my_colors_p2 <- c("#8b9faaff", "#2a5d34", "#d6a941")
#
## LOAD TABLES ##
#
DB_PATH <- '/Users/ss60/data/GTEx-comparison/gtex-comparison.db'

con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = DB_PATH)
table_names <- duckdb::sql_query("SHOW TABLES;", con)

#
## COMPARE NUMBERS
#

stats <- dplyr::tbl(con, "gtex_stats") |>
  dplyr::collect() |>
  dplyr::mutate(
    label = dplyr::case_when(
      surplus != "+0.0%" ~
        stringr::str_glue("{number}\n({surplus})"),
      TRUE ~ number
    )
  )


p1 = ggplot2::ggplot(
  data = stats,
  mapping = ggplot2::aes(x = dataset, y = cnt, fill = release)
) +
  ggplot2::geom_col(position = ggplot2::position_dodge(width = 0.9)) +
  ggplot2::theme_minimal() +
  ggplot2::scale_fill_manual(values = my_colors_p1) +
  ggplot2::theme(
    axis.text.y = element_text(angle = 90, hjust = 0.5),
    legend.position = "none"
  ) +
  ggplot2::scale_y_continuous(labels = scales::label_scientific()) +
  ggplot2::geom_text(
    aes(label = label),
    vjust = 1.2,
    size = 4,
    fontface = "bold",
    color = 'white',
    position = ggplot2::position_dodge(width = 0.9)
  ) +
  ggplot2::ggtitle("Numbers of studies/credible sets")


#
## COMPARE ALL VARIANTS
#

variant_counts <- dplyr::tbl(con, "gtex_variant_count") |>
  dplyr::collect() |>
  dplyr::mutate(
    label = dplyr::case_when(
      surplus != "+0.0%" ~
        stringr::str_glue("{number}\n({surplus})"),
      TRUE ~ number
    )
  )


variant_per_release <- dplyr::tbl(con, "gtex_variant_overlap") |>
  dplyr::collect() |>
  dplyr::mutate(
    release = purrr::map_chr(releases, function(r) {
      if (length(r) == 2) {
        return("both")
      }
      stringr::str_interp("${r}-only")
    })
  ) |>
  dplyr::mutate(number = format(cnt, big.mark = ",", scientific = FALSE)) |>
  dplyr::select(release, number, cnt)


#
## Per release distinct counts
#

p2 <- ggplot2::ggplot(
  data = variant_counts,
  mapping = ggplot2::aes(y = cnt, x = release, fill = release)
) +
  ggplot2::geom_col() +
  ggplot2::theme_minimal() +
  ggplot2::scale_fill_manual(values = my_colors_p1) +
  ggplot2::theme(
    axis.text.y = element_text(angle = 90, hjust = 0.5),
    legend.position = "none"
  ) +
  ggplot2::scale_y_continuous(labels = scales::label_scientific()) +
  ggplot2::geom_text(
    aes(label = label),
    vjust = 1.5,
    size = 4,
    fontface = "bold",
    color = 'white',
  ) +

  ggplot2::ggtitle("Total # variantIds")

p3 <- ggplot2::ggplot(
  data = variant_per_release,
  mapping = ggplot2::aes(y = cnt, x = release, fill = release)
) +
  ggplot2::geom_col() +
  ggplot2::theme_minimal() +
  ggplot2::scale_fill_manual(values = my_colors_p2) +
  ggplot2::theme(axis.text.y = element_text(angle = 90, hjust = 0.5)) +
  ggplot2::scale_y_continuous(labels = scales::label_scientific()) +
  ggplot2::xlab("overlap") +
  ggplot2::geom_text(
    aes(label = number),
    vjust = 1.5,
    size = 4,
    fontface = "bold",
    color = 'white',
  ) +
  ggplot2::ggtitle("Overlapping/non-overlapping # variantIds")

library(patchwork)

p4 = (p1 / (p2 | p3)) +
  patchwork::plot_annotation("GTEx v8 vs GTEx v10")
p4

#
## quant method
#

#
## sample size
#

sample_size <- dplyr::tbl(con, "gtex_sample_size") |>
  dplyr::collect() |>
  dplyr::mutate(
    avg = as.integer(avg),
    std = as.integer(std),
    min = as.integer(min),
    max = as.integer(max),
    q25 = as.integer(q25),
    q50 = as.integer(q50),
    q75 = as.integer(q75),
  )

p5 <- ggplot2::ggplot(
  data = sample_size,
  mapping = ggplot2::aes(
    x = release,
    ymin = min,
    lower = q25,
    middle = q50,
    upper = q75,
    max = max
  )
) +
  ggplot2::geom_boxplot(stat = "identity") +
  ggplot2::labs(
    title = "Sample Sizes by Release Type",
    x = "Release",
    y = "Sample Size"
  ) +
  ggplot2::theme_minimal() +
  ggplot2::theme(
    axis.text.x = ggplot2::element_text(hjust = 1)
  ) +
  ggplot2::stat_summary(
    fun = max,
    geom = "text",
    mapping = ggplot2::aes(label = max),
    vjust = -0.5,
    color = "red",
    size = 3,
    fontface = "bold"
  ) +
  ggplot2::stat_summary(
    fun = min,
    geom = "text",
    mapping = ggplot2::aes(label = min),
    vjust = 1.5,
    color = "blue",
    size = 3,
    fontface = "bold"
  ) +
  ggplot2::stat_summary(
    fun = median,
    geom = "text",
    mapping = ggplot2::aes(label = q50),
    vjust = -0.5,
    color = "darkgreen",
    size = 3,
    fontface = "bold"
  ) +
  ggplot2::labs(
    title = "Sample Sizes by Release Type",
    x = "Release",
    y = "Sample Size"
  )

p5
