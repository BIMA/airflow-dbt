{{
  config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by={
      "field": "created_at_wib",
      "data_type": "datetime",
      "granularity": "day",
    },
    full_refresh = false,
    tags=['P1']
  )
}}

SELECT 1 FROM {{ source("sta_datalake", "payment") }}