import os
import click
from cli.utils import constants

GA4_TRAINING_PIPELINE = """{{
  {training_params}
  ],
  "jobs": [
    {{
      "hash_start_conditions": [],
      "worker_class": "BQMLTrainer",
      "params": [
        {{
          "description": null,
          "value": "{training_query}",
          "label": "Query",
          "is_required": false,
          "type": "sql",
          "name": "query"
        }},
        {{
          "description": null,
          "value": "{crmint_project}",
          "label": "BQ Project ID",
          "is_required": false,
          "type": "string",
          "name": "bq_project_id"
        }},
        {{
          "description": null,
          "value": "{{% BQ_DATASET_LOCATION %}}",
          "label": "BQ Dataset Location",
          "is_required": false,
          "type": "string",
          "name": "bq_dataset_location"
        }}
      ],
      "id": "training_query",
      "name": "{training_name}"
      }}
  ],
  "name": "{pipeline_name}",
  "schedules": [
      {{
          "cron": "0 0 * * 0"
      }}
  ]
  }}""".strip()

GA4_PREDICTION_PIPELINE = """{{
    {prediction_params}
    ],
    "jobs": [
      {{
        "hash_start_conditions": [],
        "worker_class": "BQQueryLauncher",
        "params": [
          {{
            "description": null,
            "value": "{prediction_query}",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_predictions",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
              "description": null,
              "value": "{{% BQ_DATASET_LOCATION %}}",
              "label": "BQ Dataset Location",
              "is_required": false,
              "type": "string",
              "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
          }}
        ],
        "id": "predict",
        "name": "Predict"
      }},
      {{
        "hash_start_conditions": [
          {{
            "preceding_job_id": "predict",
            "condition": "success"
          }}
        ],
        "worker_class": "BQQueryLauncher",
        "params": [
          {{
            "description": null,
            "value": "{extract_query}",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_measurement_protocol_formatted",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
          }}
        ],
        "id": "extract",
        "name": "Extract Scores"
      }},
      {{
        "hash_start_conditions": [
          {{
            "preceding_job_id": "extract",
            "condition": "success"
          }}
        ],
        "worker_class": "BQToMeasurementProtocolGA4",
        "params": [
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_measurement_protocol_formatted",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
              "description": null,
              "value": "{{% BQ_DATASET_LOCATION %}}",
              "label": "BQ Dataset Location",
              "is_required": false,
              "type": "string",
              "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": "{{% MEASUREMENT_ID %}}",
            "label": "Measurement ID",
            "is_required": false,
            "type": "string",
            "name": "measurement_id"
          }},
          {{
            "description": null,
            "value": "{{% API_SECRET %}}",
            "label": "API Secret",
            "is_required": false,
            "type": "string",
            "name": "api_secret"
          }},
          {{
            "description": null,
            "value": "{{\\n  \\"client_id\\": \\"%(client_id)s\\",\\n  \\"timestamp_micros\\": \\"%(event_timestamp)s\\",\\n  \\"nonPersonalizedAds\\": false,\\n  \\"events\\": [\\n    {{\\n      \\"name\\": \\"post_score\\",\\n      \\"params\\": {{\\n        \\"score\\": \\"%(score)s\\",\\n        \\"model_type\\": \\"%(model_type)s\\"\\n      }}\\n    }}\\n  ]\\n}}",
            "label": "GA4 Measurement Protocol JSON template",
            "is_required": false,
            "type": "text",
            "name": "template"
          }},
          {{
              "description": null,
              "value": "{{% BQ_DATASET_LOCATION %}}",
              "label": "BQ Dataset Location",
              "is_required": false,
              "type": "string",
              "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": "10",
            "label": "Measurement Protocol batch size",
            "is_required": false,
            "type": "number",
            "name": "mp_batch_size"
          }},
          {{
            "description": null,
            "value": false,
            "label": "Debug mode",
            "is_required": false,
            "type": "boolean",
            "name": "debug"
          }}
        ],
        "id": "ga4_measurement_protocol",
        "name": "Send Events to GA4"
      }}
    ],
  "name": "{pipeline_name}",
  "schedules": [
    {{
      "cron": "0 0 * * *"
    }}
  ]
}}""".strip()

GA4_PARAMS = """"
  params": [
    {{
        "type": "text",
        "name": "BQ_PROJECT",
        "value": "{bq_project_id}"
    }},
    {{
        "type": "text",
        "name": "BQ_DATASET",
        "value": "{bq_dataset_id}"
    }},
    {{
        "type": "text",
        "name": "BQ_NAMESPACE",
        "value": "{bq_namespace}"
    }},
    {{
        "type": "text",
        "name": "BQ_DATASET_LOCATION",
        "value": "{bq_dataset_location}"
    }},
    {{
        "type": "text",
        "name": "MEASUREMENT_ID",
        "value": "{ga_measurement_id}"
    }},
    {{
        "type": "text",
        "name": "API_SECRET",
        "value": "{ga_api_secret}"
    }}"""
GA4_EXTRACT_QUERY = """#standardSQL\\r\\nWITH\\r\\n  maxDate AS (\\r\\n    SELECT SUBSTR(MAX(table_id), LENGTH('{table_suffix}') + 1) AS latest\\r\\n    FROM `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.__TABLES_SUMMARY__`\\r\\n    WHERE table_id LIKE '{table_suffix}%'\\r\\n  ),\\r\\n  visitorsWithScoreYesterday AS (\\r\\n    SELECT\\r\\n      user_pseudo_id\\r\\n    FROM\\r\\n      `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*` AS GA,\\r\\n      UNNEST(event_params) AS EP\\r\\n    WHERE\\r\\n      event_name = 'post_score'\\r\\n      AND EP.value.string_value = '{model_objective} - Instant BQML'\\r\\n      AND _TABLE_SUFFIX = FORMAT_DATE(\\r\\n        '%Y%m%d',\\r\\n        DATE_SUB(\\r\\n          PARSE_DATE('%Y%m%d', (SELECT latest FROM maxDate)),\\r\\n          INTERVAL 1 DAY))\\r\\n    GROUP BY 1\\r\\n  ),\\r\\n  visitorsNeedingScoreYesterday AS (\\r\\n    SELECT\\r\\n      user_pseudo_id AS visitor\\r\\n    FROM\\r\\n      `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*`\\r\\n    WHERE\\r\\n      _TABLE_SUFFIX = FORMAT_DATE(\\r\\n        '%Y%m%d',\\r\\n        DATE_SUB(\\r\\n          PARSE_DATE('%Y%m%d', (SELECT latest FROM maxDate)),\\r\\n          INTERVAL 1 DAY))\\r\\n      AND user_pseudo_id NOT IN (\\r\\n        SELECT user_pseudo_id FROM visitorsWithScoreYesterday\\r\\n      )\\r\\n    GROUP BY 1\\r\\n  )\\r\\nSELECT\\r\\n  'post_score' AS event_name,\\r\\n  Predict.user_pseudo_id AS client_id,\\r\\n  normalizedScore AS score,\\r\\n  '{model_objective} - Instant BQML' AS model_type,\\r\\n  Timestamps.event_timestamp AS event_timestamp\\r\\nFROM\\r\\n  (\\r\\n    SELECT\\r\\n      predicted_will_convert_later,\\r\\n      user_pseudo_id,\\r\\n      NTILE(1000)\\r\\n        OVER (ORDER BY predicted_will_convert_later ASC) AS normalizedScore\\r\\n    FROM\\r\\n      `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_predictions`\\r\\n        AS P\\r\\n    GROUP BY 1, 2\\r\\n  ) AS Predict\\r\\nINNER JOIN (SELECT visitor FROM visitorsNeedingScoreYesterday) AS Visitors\\r\\n  ON Predict.user_pseudo_id = Visitors.visitor\\r\\nLEFT JOIN\\r\\n  (\\r\\n    SELECT\\r\\n      user_pseudo_id,\\r\\n      MAX(event_timestamp) AS event_timestamp\\r\\n    FROM\\r\\n      `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*`\\r\\n    WHERE\\r\\n      _TABLE_SUFFIX = FORMAT_DATE(\\r\\n        '%Y%m%d',\\r\\n        DATE_SUB(\\r\\n          PARSE_DATE('%Y%m%d', (SELECT latest FROM maxDate)),\\r\\n          INTERVAL 1 DAY))\\r\\n    GROUP BY 1\\r\\n  ) AS Timestamps\\r\\n  ON Predict.user_pseudo_id = Timestamps.user_pseudo_id\\r\\nGROUP BY 1, 2, 3, 4, 5;"""
GA4_PREDICTION_QUERY = """SELECT user_pseudo_id, predicted_will_convert_later\\r\\nFROM\\r\\n    ml.predict(\\r\\n        MODEL `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_model`,\\r\\n        (\\r\\n            WITH \\r\\n                visitors_labeled AS ( \\r\\n                    SELECT\\r\\n                        user_pseudo_id, \\r\\n                        MIN(\\r\\n                            CASE \\r\\n                            WHEN {objective}\\r\\n                            THEN event_timestamp END) AS event_session, \\r\\n                        MIN(\\r\\n                            CASE \\r\\n                            WHEN {objective}\\r\\n                            THEN event_date END) AS event_date, \\r\\n                        MAX(\\r\\n                            CASE \\r\\n                            WHEN {objective}\\r\\n                            THEN 1 \\r\\n                            ELSE 0 END) AS label\\r\\n                    FROM \\r\\n                        `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*` AS GA\\r\\n                    WHERE \\r\\n                        _TABLE_SUFFIX BETWEEN\\r\\n                            FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n                    GROUP BY \\r\\n                        user_pseudo_id\\r\\n                )\\r\\n                SELECT \\r\\n                    GA.user_pseudo_id,\\r\\n                    IFNULL(MAX(label), 0) AS will_convert_later,\\r\\n                    MAX(geo.city) AS city,\\r\\n                    MAX(geo.region) AS region,\\r\\n                    MAX(traffic_source.medium) AS medium,\\r\\n                    MAX(traffic_source.source) AS source,\\r\\n                    MAX(device.web_info.browser) AS browser,\\r\\n                    COUNT(DISTINCT event_name) AS events,\\r\\n                    MAX(event_name) AS common_events,\\r\\n                    MAX(device.category) AS device_category,\\r\\n                    MAX(device.operating_system) AS device_operating_system,\\r\\n                    MAX(platform) AS platform\\r\\n                FROM \\r\\n                    `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*` AS GA\\r\\n                LEFT JOIN visitors_labeled AS Labels\\r\\n                    ON GA.user_pseudo_id = Labels.user_pseudo_id\\r\\n                WHERE \\r\\n                    _TABLE_SUFFIX BETWEEN\\r\\n                        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                        AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n                    AND (\\r\\n                        GA.event_timestamp < IFNULL(event_session, 0)\\r\\n                        OR event_session IS NULL)\\r\\n                GROUP BY \\r\\n                    GA.user_pseudo_id\\r\\n        )\\r\\n    );"""
GA4_TRAINING_QUERY = """#standardSQL\\r\\n{create_dataset}CREATE OR REPLACE MODEL `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_model\`\\r\\n    OPTIONS ({model_options})\\r\\nAS (\\r\\n    WITH \\r\\n        visitors_labeled AS ( \\r\\n            SELECT\\r\\n                user_pseudo_id, \\r\\n                MIN(\\r\\n                    CASE \\r\\n                    WHEN {objective}\\r\\n                    THEN event_timestamp END) AS event_session, \\r\\n                MIN(\\r\\n                    CASE \\r\\n                    WHEN {objective}\\r\\n                    THEN event_date END) AS event_date, \\r\\n                MAX(\\r\\n                    CASE \\r\\n                    WHEN {objective}\\r\\n                    THEN 1 \\r\\n                    ELSE 0 END) AS label\\r\\n            FROM \\r\\n                `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*` AS GA\\r\\n            WHERE \\r\\n                _TABLE_SUFFIX BETWEEN\\r\\n                    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            GROUP BY \\r\\n                user_pseudo_id\\r\\n        ),\\r\\n        user_model AS (\\r\\n            SELECT \\r\\n                GA.user_pseudo_id,\\r\\n                IFNULL(MAX(label), 0) AS will_convert_later,\\r\\n                MAX(geo.city) AS city,\\r\\n                MAX(geo.region) AS region,\\r\\n                MAX(traffic_source.medium) AS medium,\\r\\n                MAX(traffic_source.source) AS source,\\r\\n                MAX(device.web_info.browser) AS browser,\\r\\n                COUNT(DISTINCT event_name) AS events,\\r\\n                MAX(event_name) AS common_events,\\r\\n                MAX(device.category) AS device_category,\\r\\n                MAX(device.operating_system) AS device_operating_system,\\r\\n                MAX(platform) AS platform\\r\\n            FROM \\r\\n                `{ga4_bigquery_export_project}.{{% BQ_DATASET %}}.{table_suffix}*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n                ON GA.user_pseudo_id = Labels.user_pseudo_id\\r\\n            WHERE \\r\\n                _TABLE_SUFFIX BETWEEN\\r\\n                    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n                AND (\\r\\n                    GA.event_timestamp < IFNULL(event_session, 0)\\r\\n                    OR event_session IS NULL)\\r\\n            GROUP BY \\r\\n                GA.user_pseudo_id\\r\\n        )\\r\\n    SELECT\\r\\n        *\\r\\n        EXCEPT (user_pseudo_id)\\r\\n    FROM\\r\\n        user_model\\r\\n    LIMIT 100000000\\r\\n);"""

UA_TRAINING_PIPELINE = """{{
    {training_params}
    ],
    "jobs": [
      {{
        "hash_start_conditions": [],
        "worker_class": "BQMLTrainer",
        "params": [
          {{
            "description": null,
            "value": "{training_query}",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
              "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }}
        ],
        "id": "training_query",
        "name": "{training_name}"
      }}
    ],
    "name": "{training_pipeline_name} - Audiences",
    "schedules": [
      {{
        "cron": "0 0 * * 0"
      }}
    ]
}}""".strip()

UA_PREDICTION_PIPELINE = """{{
  {prediction_params}
  ],
  "jobs": [
    {{
        "hash_start_conditions": [],
        "worker_class": "BQQueryLauncher",
        "params": [
        {{
            "description": null,
            "value": "{prediction_query}",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
        }},
        {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_predictions",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
        }},
        {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
        }}
        ],
        "id": "predict",
        "name": "Predict"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "predict",
            "condition": "success"
        }}
        ],
        "worker_class": "BQQueryLauncher",
        "params": [
        {{
            "description": null,
            "value": "SELECT\\r\\n    ga.custom_dimension_userId,\\r\\n    predict.prediction AS score,\\r\\n    NTILE(1000) OVER (ORDER BY predict.prediction ASC) AS tile\\r\\nFROM\\r\\n    (\\r\\n        SELECT\\r\\n            {cid},\\r\\n            predicted_will_convert_later AS prediction\\r\\n        FROM\\r\\n            `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_predictions`\\r\\n            AS P\\r\\n    ) AS predict\\r\\nINNER JOIN\\r\\n    (\\r\\n        SELECT\\r\\n            {cid},\\r\\n            {scope_query} AS ga\\r\\n    ON predict.{cid} = ga.{cid};",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
        }},
        {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_data_import_staging",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
        }},
        {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
        }}
        ],
        "id": "extract_scores",
        "name": "Extract Scores"
    }},
    {{
        "hash_start_conditions": [
          {{
            "preceding_job_id": "extract_scores",
            "condition": "success"
          }}
        ],
        "worker_class": "BQQueryLauncher",
        "params": [
          {{
            "description": null,
            "value": "#StandardSQL\\r\\nSELECT\\r\\n  'ga:dimension{{% CD_USER_ID %}}' AS ga_dimension{{% CD_USER_ID %}},\\r\\n  'ga:dimension{{% CD_SCORE %}}' AS ga_dimension{{% CD_SCORE %}};\\r\\n",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_data_import_formatted",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
          }}
        ],
        "id": "data_import_headers",
        "name": "Data Import Headers"
    }},
    {{
        "hash_start_conditions": [
          {{
            "preceding_job_id": "data_import_headers",
            "condition": "success"
          }}
        ],
        "worker_class": "BQQueryLauncher",
        "params": [
          {{
            "description": null,
            "value": "SELECT\\r\\n  custom_dimension_userid AS ga_dimension{{% CD_USER_ID %}},\\r\\n  CAST(tile AS STRING) AS ga_dimension{{% CD_SCORE %}}\\r\\nFROM\\r\\n  `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_data_import_staging`\\r\\nGROUP BY 1, 2;",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_data_import_formatted",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": false,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
          }}
        ],
        "id": "data_import_scores",
        "name": "Data Import Scores"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "data_import_scores",
            "condition": "success"
        }}
        ],
        "worker_class": "BQToStorageExporter",
        "params": [
        {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_data_import_formatted",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
        }},
        {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
        }},
        {{
            "description": null,
            "value": "gs://{{% GCS_BUCKET %}}/predictions_{{% today(\\"%Y%m%d\\") %}}.csv",
            "label": "Destination CSV or JSON file URI (e.g. gs://bucket/data.csv)",
            "is_required": false,
            "type": "string",
            "name": "destination_uri"
        }},
        {{
            "description": null,
            "value": false,
            "label": "Include a header row",
            "is_required": false,
            "type": "boolean",
            "name": "print_header"
        }},
        {{
            "description": null,
            "value": false,
            "label": "Export in JSON format",
            "is_required": false,
            "type": "boolean",
            "name": "export_json"
        }}
        ],
        "id": "data_import_file_creation",
        "name": "Load File to Storage"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "data_import_file_creation",
            "condition": "success"
        }}
        ],
        "worker_class": "GADataImporter",
        "params": [
        {{
            "description": null,
            "value": "gs://{{% GCS_BUCKET %}}/predictions_{{% today(\\"%Y%m%d\\") %}}.csv",
            "label": "CSV data file URI (e.g. gs://bucket/data.csv)",
            "is_required": false,
            "type": "string",
            "name": "csv_uri"
        }},
        {{
            "description": null,
            "value": "{{% GA_PROPERTY_ID %}}",
            "label": "GA Property Tracking ID (e.g. UA-12345-3)",
            "is_required": false,
            "type": "string",
            "name": "property_id"
        }},
        {{
            "description": null,
            "value": "{{% GA_DATASET_ID %}}",
            "label": "GA Dataset ID (e.g. sLj2CuBTDFy6CedBJw)",
            "is_required": false,
            "type": "string",
            "name": "dataset_id"
        }},
        {{
            "description": null,
            "value": "1",
            "label": "Maximum uploads to keep in GA Dataset (leave empty to keep all)",
            "is_required": false,
            "type": "number",
            "name": "max_uploads"
        }},
        {{
            "description": null,
            "value": true,
            "label": "Delete older uploads before upload",
            "is_required": false,
            "type": "boolean",
            "name": "delete_before"
        }},
        {{
            "description": null,
            "value": "{{% GA_ACCOUNT_ID %}}",
            "label": "GA Account ID",
            "is_required": false,
            "type": "string",
            "name": "account_id"
        }}
        ],
        "id": "import_scores",
        "name": "Import Scores to GA"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "import_scores",
            "condition": "success"
        }}
        ],
        "worker_class": "StorageCleaner",
        "params": [
        {{
            "description": null,
            "value": "gs://{{% GCS_BUCKET %}}/predictions_*.csv",
            "label": "List of file URIs and URI patterns (e.g. gs://bucket/data.csv or gs://bucket/data_*.csv)",
            "is_required": false,
            "type": "string_list",
            "name": "file_uris"
        }},
        {{
            "description": null,
            "value": "10",
            "label": "Days to keep files since last modification",
            "is_required": false,
            "type": "number",
            "name": "expiration_days"
        }}
        ],
        "id": "cloud_storage_cleaner",
        "name": "Storage Cleaner"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "cloud_storage_cleaner",
            "condition": "success"
        }}
        ],
        "worker_class": "BQQueryLauncher",
        "params": [
          {{
            "description": null,
            "value": "#standardsql\\r\\nWITH\\r\\n  Percentiles AS (\\r\\n    SELECT\\r\\n      score,\\r\\n      custom_dimension_userId,\\r\\n      NTILE(1000) OVER (ORDER BY score ASC) AS tile\\r\\n    FROM\\r\\n      `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_data_import_staging`\\r\\n    GROUP BY 1, 2\\r\\n  ),\\r\\n  MinScores AS (\\r\\n    SELECT\\r\\n      tile,\\r\\n      MIN(score) AS min_score\\r\\n    FROM\\r\\n      Percentiles\\r\\n    GROUP BY 1\\r\\n  ),\\r\\n  UserCount AS (\\r\\n    SELECT COUNT(DISTINCT custom_dimension_userId) AS user_count\\r\\n    FROM\\r\\n      `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_data_import_staging`\\r\\n  ),\\r\\n  Analysis AS (\\r\\n    SELECT \\r\\n      tile,\\r\\n      user_count,\\r\\n      users_in_this_tile,\\r\\n      min_score,\\r\\n      SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) AS running_total,\\r\\n      SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count AS proportion_users,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.1) AS a_10p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.2) AS a_20p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.3) AS a_30p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.4) AS a_40p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.5) AS a_50p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.6) AS a_60p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.7) AS a_70p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.8) AS a_80p,\\r\\n      ABS((SUM(users_in_this_tile) OVER (\\r\\n          ROWS BETWEEN UNBOUNDED PRECEDING\\r\\n          AND CURRENT ROW) / user_count) - 0.9) AS a_90p\\r\\n    FROM (\\r\\n      SELECT \\r\\n        tile,\\r\\n        user_count,\\r\\n        users_in_this_tile,\\r\\n        min_score\\r\\n      FROM (\\r\\n        SELECT \\r\\n          P.tile, \\r\\n          M.min_score,\\r\\n          U.user_count,\\r\\n          COUNT(DISTINCT custom_dimension_userId) AS users_in_this_tile\\r\\n        FROM\\r\\n          Percentiles AS P\\r\\n        CROSS JOIN (SELECT user_count FROM UserCount) AS U\\r\\n        LEFT JOIN (\\r\\n          SELECT tile, min_score \\r\\n          FROM MinScores\\r\\n        ) AS M\\r\\n        ON M.tile = P.tile\\r\\n        GROUP BY 1, 2, 3\\r\\n      )\\r\\n      GROUP BY 1, 2, 3, 4\\r\\n      ORDER BY tile DESC\\r\\n    )\\r\\n  )\\r\\nSELECT\\r\\n  tier,\\r\\n  lower_threshold,\\r\\n  upper_threshold,\\r\\n  CASE\\r\\n    WHEN LENGTH(lower_threshold) = 1\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(lower_threshold, 0, 1),\\r\\n          '-9]$|[1-9][0-9]$|[0-9][0-9][0-9]$|[0-9]{{4}})')\\r\\n    WHEN LENGTH(lower_threshold) = 2\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(lower_threshold, 0, 1),\\r\\n          '][',\\r\\n          SUBSTRING(lower_threshold, -1, 1),\\r\\n          '-9]$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(lower_threshold, 0, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(lower_threshold, 0, 1),\\r\\n            CAST(\\r\\n              CAST(SUBSTRING(lower_threshold, 0, 1) AS INT64) + 1 AS STRING)),\\r\\n          '-9][',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(lower_threshold, -1, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(lower_threshold, -1, 1),\\r\\n            '0'),\\r\\n          '-9]$|[0-9]{{3}}$|[0-9]{{4}})')\\r\\n    WHEN LENGTH(lower_threshold) = 3\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(lower_threshold, 0, 1),\\r\\n          '-9][',\\r\\n          SUBSTRING(lower_threshold, 2, 1),\\r\\n          '-9][',\\r\\n          SUBSTRING(lower_threshold, -1, 1),\\r\\n          '-9]$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(lower_threshold, 2, 1) AS INT64) = 9\\r\\n              AND CAST(SUBSTRING(lower_threshold, -1, 1) AS INT64) = 9,\\r\\n            CONCAT(SUBSTRING(lower_threshold, 0, 1), '-9][9-9][9-9]'),\\r\\n            CONCAT(\\r\\n              SUBSTRING(lower_threshold, 0, 1),\\r\\n              '-9][',\\r\\n              CAST(\\r\\n                CAST(SUBSTRING(lower_threshold, 2, 1) AS INT64) + 1 AS STRING),\\r\\n              '-9][0-9]')),\\r\\n          '$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(lower_threshold, 0, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(lower_threshold, 0, 1),\\r\\n            CAST(\\r\\n              CAST(SUBSTRING(lower_threshold, 0, 1) AS INT64) + 1 AS STRING)),\\r\\n          '-9][0-9][0-9]$|[0-9]{{4}})')\\r\\n    END AS lower_threshold_audience_regex,\\r\\n  CASE\\r\\n    WHEN upper_threshold IS NULL THEN '$^'\\r\\n    WHEN LENGTH(upper_threshold) = 1\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(upper_threshold, 0, 1),\\r\\n          '-9]$|[1-9][0-9]$|[0-9][0-9][0-9]$|[0-9]{{4}})')\\r\\n    WHEN LENGTH(upper_threshold) = 2\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(upper_threshold, 0, 1),\\r\\n          '][',\\r\\n          SUBSTRING(upper_threshold, -1, 1),\\r\\n          '-9]$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(upper_threshold, 0, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(upper_threshold, 0, 1),\\r\\n            CAST(\\r\\n              CAST(SUBSTRING(upper_threshold, 0, 1) AS INT64) + 1 AS STRING)),\\r\\n          '-9][',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(upper_threshold, -1, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(upper_threshold, -1, 1),\\r\\n            '0'),\\r\\n          '-9]$|[0-9]{{3}}$|[0-9]{{4}})')\\r\\n    WHEN LENGTH(upper_threshold) = 3\\r\\n      THEN\\r\\n        CONCAT(\\r\\n          '^([',\\r\\n          SUBSTRING(upper_threshold, 0, 1),\\r\\n          '-9][',\\r\\n          SUBSTRING(upper_threshold, 2, 1),\\r\\n          '-9][',\\r\\n          SUBSTRING(upper_threshold, -1, 1),\\r\\n          '-9]$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(upper_threshold, 2, 1) AS INT64) = 9\\r\\n              AND CAST(SUBSTRING(upper_threshold, -1, 1) AS INT64) = 9,\\r\\n            CONCAT(SUBSTRING(upper_threshold, 0, 1), '-9][9-9][9-9]'),\\r\\n            CONCAT(\\r\\n              SUBSTRING(upper_threshold, 0, 1),\\r\\n              '-9][',\\r\\n              CAST(\\r\\n                CAST(SUBSTRING(upper_threshold, 2, 1) AS INT64) + 1 AS STRING),\\r\\n              '-9][0-9]')),\\r\\n          '$|[',\\r\\n          IF(\\r\\n            CAST(SUBSTRING(upper_threshold, 0, 1) AS INT64) + 1 = 10,\\r\\n            SUBSTRING(upper_threshold, 0, 1),\\r\\n            CAST(\\r\\n              CAST(SUBSTRING(upper_threshold, 0, 1) AS INT64) + 1 AS STRING)),\\r\\n          '-9][0-9][0-9]$|[0-9]{{4}})')\\r\\n    END AS upper_threshold_audience_regex\\r\\nFROM\\r\\n  (\\r\\n    SELECT\\r\\n      NULL AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_10p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 1' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_10p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_20p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 2' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_20p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_30p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 3' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_30p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_40p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 4' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_40p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_50p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 5' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_50p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_60p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 6' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_60p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_70p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 7' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_70p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_80p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 8' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_80p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_90p ASC\\r\\n        LIMIT 1\\r\\n      ) AS lower_threshold,\\r\\n      'Decile 9' AS tier\\r\\n    UNION ALL\\r\\n    SELECT\\r\\n      (\\r\\n        SELECT CAST(tile AS STRING)\\r\\n        FROM Analysis\\r\\n        ORDER BY a_90p ASC\\r\\n        LIMIT 1\\r\\n      ) AS upper_threshold,\\r\\n      '0' AS lower_threshold,\\r\\n      'Decile 10' AS tier\\r\\n  );",
            "label": "Query",
            "is_required": false,
            "type": "sql",
            "name": "query"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_audience_boundaries",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": true,
            "label": "Overwrite table",
            "is_required": false,
            "type": "boolean",
            "name": "overwrite"
          }}
        ],
        "id": "audience_boundaries",
        "name": "Audience Boundaries"
    }},
    {{
        "hash_start_conditions": [
        {{
            "preceding_job_id": "audience_boundaries",
            "condition": "success"
        }}
        ],
        "worker_class": "GAAudiencesUpdater",
        "params": [
          {{
            "description": null,
            "value": "{{% GA_PROPERTY_ID %}}",
            "label": "GA Property Tracking ID (e.g. UA-12345-3)",
            "is_required": false,
            "type": "string",
            "name": "property_id"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_audience_boundaries",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": "{{\\r\\n  \\"audienceType\\": \\"SIMPLE\\",\\r\\n  \\"linkedViews\\": [\\r\\n    {{% BQ_DATASET %}}\\r\\n  ],\\r\\n  \\"name\\": \\"%(tier)s - {model_objective} - Instant BQML\\",\\r\\n  \\"audienceDefinition\\": {{\\r\\n    \\"includeConditions\\": {{\\r\\n      \\"daysToLookBack\\": 30,\\r\\n      \\"segment\\": \\"users::condition::ga:dimension{{% CD_SCORE %}}=~%(lower_threshold_audience_regex)s;ga:dimension{{% CD_SCORE %}}!~%(upper_threshold_audience_regex)s\\",\\r\\n      \\"isSmartList\\": false,\\r\\n      \\"membershipDurationDays\\": 30\\r\\n    }}\\r\\n  }},\\r\\n  \\"linkedAdAccounts\\": [\\r\\n    {{\\r\\n      \\"linkedAccountId\\": \\"{linked_account_id}\\",\\r\\n      \\"type\\": \\"{linked_account_type}\\"\\r\\n    }}\\r\\n  ]\\r\\n}}",
            "label": "GA audience JSON template",
            "is_required": false,
            "type": "text",
            "name": "template"
          }},
          {{
            "description": null,
            "value": "{{% GA_ACCOUNT_ID %}}",
            "label": "GA Account ID",
            "is_required": false,
            "type": "string",
            "name": "account_id"
          }}
        ],
        "id": "publish_ga_audience",
        "name": "Publish GA Audiences"
      }},
      {{
        "hash_start_conditions": [
          {{
            "preceding_job_id": "publish_ga_audience",
            "condition": "whatever"
          }}
        ],
        "worker_class": "GAAudiencesUpdater",
        "params": [
          {{
            "description": null,
            "value": "{{% GA_PROPERTY_ID %}}",
            "label": "GA Property Tracking ID (e.g. UA-12345-3)",
            "is_required": false,
            "type": "string",
            "name": "property_id"
          }},
          {{
            "description": null,
            "value": "{crmint_project}",
            "label": "BQ Project ID",
            "is_required": false,
            "type": "string",
            "name": "bq_project_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET %}}",
            "label": "BQ Dataset ID",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_NAMESPACE %}}_audience_boundaries",
            "label": "BQ Table ID",
            "is_required": false,
            "type": "string",
            "name": "bq_table_id"
          }},
          {{
            "description": null,
            "value": "{{% BQ_DATASET_LOCATION %}}",
            "label": "BQ Dataset Location",
            "is_required": false,
            "type": "string",
            "name": "bq_dataset_location"
          }},
          {{
            "description": null,
            "value": "{{\\r\\n  \\"name\\": \\"%(tier)s - {model_objective} - Instant BQML\\",\\r\\n  \\"audienceDefinition\\": {{\\r\\n    \\"includeConditions\\": {{\\r\\n      \\"segment\\": \\"users::condition::ga:dimension{{% CD_SCORE %}}=~%(lower_threshold_audience_regex)s;ga:dimension{{% CD_SCORE %}}!~%(upper_threshold_audience_regex)s\\"\\r\\n    }}\\r\\n  }}\\r\\n}}",
            "label": "GA audience JSON template",
            "is_required": false,
            "type": "text",
            "name": "template"
          }},
          {{
            "description": null,
            "value": "{{% GA_ACCOUNT_ID %}}",
            "label": "GA Account ID",
            "is_required": false,
            "type": "string",
            "name": "account_id"
          }}
        ],
        "id": "update_ga_audience",
        "name": "Update GA Audiences"
      }}
    ],
    "name": "{prediction_pipeline_name} - Audiences",
    "schedules": [
    {{
        "cron": "0 0 * * * *  "
    }}
    ]
}}""".strip()
GA4_MODEL_OBJECTIVES = ['Purchase Propensity', 'Event Propensity']
UA_MODEL_OBJECTIVES = ['Purchase Propensity', 'Repeat Purchase Propensity',
                       'Event Propensity', 'Destination Propensity',
                       'Product Propensity', 'Custom Dimension Propensity']

def _format_heading(label, color):
  centered = label.center(40)
  msg = click.style('============================================\n', fg=color)
  msg += click.style(f'= {centered} =\n', fg=color, bold=True)
  msg += click.style('============================================', fg=color)
  click.echo(msg)

def _model_objectives(model_objectives):
  _format_heading('Marketing Objective', 'green')
  for i, o in enumerate(model_objectives):
    click.echo(f'{i + 1}) {o}')
  return click.prompt(
    'Enter the index of the marketing objective', type=int) - 1

def _ua_event_propensity_config():
  _format_heading('Event Details', 'green')
  event_category = click.prompt(
    'What is the event category (required)', type=str)
  event_action = click.prompt(
    'What is the event action (optional - press enter to skip)', default='.*?')
  event_label = click.prompt(
    'What is the event label (optional - press enter to skip)', default='.*?')
  return event_category, event_action, event_label

def _ga4_event_propensity_config():
  _format_heading('Event Details', 'green')
  event_name = click.prompt(
    'What is the event name', type=str)
  return event_name

def _destination_propensity_config():
  _format_heading('Destination Details', 'green')
  destination_url = click.prompt(
    'What is the destination URL', default='/ordercomplete')
  return destination_url

def _product_propensity_config():
  _format_heading('Product Details', 'green')
  product_types = ['Product Category', 'Product Brand', 'Product Name', 'Product SKU']
  for i, product in enumerate(product_types):
    click.echo(f'{i + 1}) {product}')
  product_type = click.prompt(
    'Enter the index for your product type', type=int) - 1
  p = product_types[product_type]
  product_dimension = 'product_propensity.V2ProductName'
  if p == "Product Category":
    product_dimension = 'product_propensity.V2ProductCategory'
  if p == "Product Brand":
    product_dimension = 'product_propensity.ProductBrand'
  if p == "Product SKU":
    product_dimension = 'product_propensity.productSKU'
  product = click.prompt(
    f'What is the {p}', type=str)
  return product, product_dimension
  
def _cloud_architecture(stage_name):
  _format_heading('Cloud Architecture', 'blue')
  msg = (
    f'Is the BigQuery Export located in the same\n'
    f'Google Cloud Project as the CRMint application')
  if click.confirm(msg, default=True):
    same_project = True
  else:
    same_project = False
  if same_project:
    bigquery_export_project = stage_name.project_id_gae
    create_dataset = '';
  else:
    _format_heading('Google Analytics Export Cloud project ID', 'blue')
    bigquery_export_project = click.prompt(
      'What is the Cloud Project ID for your BigQuery Export', type=str)
    create_dataset = """CREATE SCHEMA IF NOT EXISTS `{crmint_project}.{{% BQ_DATASET %}}`;\\r\\n""".format(
        crmint_project=stage_name.project_id_gae)
  return bigquery_export_project, create_dataset, same_project

def _bigquery_config():
  _format_heading('BigQuery Dataset ID', 'blue')
  bq_dataset_id = click.prompt(
    'What is your BigQuery dataset ID', type=str)
  _format_heading('BigQuery Dataset Location', 'blue')
  bq_dataset_location = click.prompt(
    'What is the location of your Google Analytics BigQuery dataset', type=str)
  return bq_dataset_id, bq_dataset_location

def _custom_dimension_propensity_config():
  cd_scopes = ['User or Session', 'Hit', 'Product']
  _format_heading('Custom Dimension Details', 'green')
  custom_dimension_index = click.prompt(
    'What is the index of your custom dimension (required)', type=int)
  custom_dimension_value = click.prompt(
    'What is the value of your custom dimension (optional - press enter to skip)', default='.*?')
  for i, cd_s in enumerate(cd_scopes):
    click.echo(f'{i + 1}) {cd_s}')
  cd_scope_ind = click.prompt(
    'Enter the index for your custom dimension scope', type=int) - 1
  cd_scope = cd_scopes[cd_scope_ind]
  if cd_scope == 'User or Session':
    cd_scope_query = 'UNNEST(customDimensions) AS cd'
  if cd_scope == 'Hit':
    cd_scope_query = 'UNNEST(hits) AS h,\\r\\n            UNNEST(h.customDimensions) AS cd'
  if cd_scope == 'Product':
    cd_scope_query = 'UNNEST(hits) AS h,\\r\\n            UNNEST(h.product) AS p,\\r\\n            UNNEST(p.customDimensions) AS cd'
  return custom_dimension_index, custom_dimension_value, cd_scope_query

def _check_ga_account_id(ga_id):
  try:
    return ga_id.split('-')[1]
  except IndexError:
    ga_account_id = click.prompt(
      'Please enter a correctly formatted Google Analytics UA ID',
      default='UA-12345678-9')
    _check_ga_account_id(ga_account_id)

def _get_ga4_config(stage_name):
  cid = 'user_pseudo_id'
  table_suffix = 'events_'
  optimize_objective = 'ecommerce.purchase_revenue > 0'
  crmint_project = stage_name.project_id_gae
  model_options = """\\r\\n        MODEL_TYPE = 'AUTOML_REGRESSOR',\\r\\n        INPUT_LABEL_COLS = ['will_convert_later'],\\r\\n        BUDGET_HOURS = 3.0"""  
  mo = _model_objectives(GA4_MODEL_OBJECTIVES)
  objective = GA4_MODEL_OBJECTIVES[mo]
  if objective == 'Event Propensity':
    event_name = _ga4_event_propensity_config()
  bigquery_export_project, create_dataset, same_project = _cloud_architecture(stage_name)
  bq_dataset_id, bq_dataset_location = _bigquery_config()
  _format_heading('Namespace', 'magenta')
  bq_namespace = click.prompt(
    'Come up with a unique namespace to keep assets\n'
    'organized (ie, acme_propensity)', type=str)
  _format_heading('GA Measurement ID', 'yellow')
  ga_measurement_id = click.prompt(
    'What the Google Analytics Measurement ID', default='G-G5YB23XTTW')
  _format_heading('GA API Secret', 'yellow')
  ga_api_secret = click.prompt(
    'What the Google Analytics API secret', type=str)
  _format_heading('GA BigQuery Frequency', 'yellow')
  frequencies = ['Daily Only', 'Daily & Streaming', 'Streaming Only']
  for i, f in enumerate(frequencies):
    click.echo(f'{i + 1}) {f}')
  ind = click.prompt(
    'Enter the index for your GA BigQuery frequency', type=int) - 1
  ga_bigquery_frequency = frequencies[ind]
  if ga_bigquery_frequency == 'Streaming':
    table_suffix = 'events_intraday_'
  if not same_project:
    bq_permissions = (
      f'\nDid you add:\n'
      f'  1) BigQuery Data Viewer &\n'
      f'  2) BigQuery User\n'
      f'permissions for the App Engine default service account\n'
      f'{crmint_project}@appspot.gserviceaccount.com to the\n'
      f'Google Cloud Platform Project "{bigquery_export_project}", yet?')
    click.confirm(bq_permissions, default=True)
  _format_heading('Done >>>> Importing Pipelines', 'green')
  ga4_params = GA4_PARAMS.format(
    bq_project_id=crmint_project,
    bq_dataset_id=bq_dataset_id,
    bq_namespace=bq_namespace,
    bq_dataset_location=bq_dataset_location,
    ga_measurement_id=ga_measurement_id,
    ga_api_secret=ga_api_secret)
  if objective == 'Event Propensity':
    event_params = """,
      {{
        "type": "text",
        "name": "EVENT_NAME",
        "value": "{event_name}"
      }}""".format(event_name=event_name)
    ga4_params += event_params
    optimize_objective = """REGEXP_CONTAINS(event_name, '(?i){{% EVENT_NAME %}}')"""
  prediction_pipeline_name = f'{objective} Prediction Pipeline'
  training_pipeline_name = f'{objective} Training Pipeline'
  training_name = f'{objective} Training'
  extract_query = GA4_EXTRACT_QUERY.format(
    table_suffix=table_suffix,
    ga4_bigquery_export_project=bigquery_export_project,
    model_objective=objective,
    crmint_project=crmint_project)
  training_query = GA4_TRAINING_QUERY.format(
    create_dataset=create_dataset,
    crmint_project=crmint_project,
    model_options=model_options,
    objective=optimize_objective,
    ga4_bigquery_export_project=bigquery_export_project,
    table_suffix=table_suffix)
  prediction_query = GA4_PREDICTION_QUERY.format(
    crmint_project=crmint_project,
    objective=optimize_objective,
    ga4_bigquery_export_project=bigquery_export_project,
    table_suffix=table_suffix)
  training = GA4_TRAINING_PIPELINE.format(
    training_params=ga4_params,
    training_query=training_query,
    crmint_project=crmint_project,
    training_name=training_name,
    pipeline_name=training_pipeline_name)
  prediction = GA4_PREDICTION_PIPELINE.format(
    prediction_params=ga4_params,
    prediction_query=prediction_query,
    crmint_project=crmint_project,
    extract_query=extract_query,
    pipeline_name=prediction_pipeline_name)
  training_filename = 'ga4_training_pipeline.json'
  prediction_filename = 'ga4_prediction_pipeline.json'
  training_filepath = os.path.join(constants.STAGE_DIR, training_filename)
  with open(training_filepath, 'w+') as fp:
    fp.write(training)
  prediction_filepath = os.path.join(constants.STAGE_DIR, prediction_filename)
  with open(prediction_filepath, 'w+') as fp:
    fp.write(prediction)
  return training_filepath, prediction_filepath
      
def _get_ua_config(stage_name):
  cid = 'clientId'
  product_dimension = ''
  cd_scope_query = ''
  crmint_project = stage_name.project_id_gae
  model_options = "\\r\\n        MODEL_TYPE = 'BOOSTED_TREE_REGRESSOR',\\r\\n        BOOSTER_TYPE = 'GBTREE',\\r\\n        MAX_ITERATIONS = 50,\\r\\n        SUBSAMPLE = 0.5,\\r\\n        NUM_PARALLEL_TREE = 2,\\r\\n        DATA_SPLIT_METHOD = 'NO_SPLIT',\\r\\n        EARLY_STOP = FALSE,\\r\\n        INPUT_LABEL_COLS = ['will_convert_later']"
  mo = _model_objectives(UA_MODEL_OBJECTIVES)
  objective = UA_MODEL_OBJECTIVES[mo]
  if objective == 'Event Propensity':
    event_category, event_action, event_label = _ua_event_propensity_config()
  if objective == 'Destination Propensity':
    destination_url = _destination_propensity_config()
  if objective == 'Product Propensity':
    product, product_dimension = _product_propensity_config()
  if objective == 'Custom Dimension Propensity':
    custom_dimension_index, custom_dimension_value, cd_scope_query = _custom_dimension_propensity_config()
  bigquery_export_project, create_dataset, same_project = _cloud_architecture(stage_name)
  bq_dataset_id, bq_dataset_location = _bigquery_config()
  _format_heading('Namespace', 'magenta')
  bq_namespace = click.prompt(
    'Come up with a unique namespace to keep assets\n'
    'organized (ie, acme_propensity)', type=str)
  _format_heading('GA Account ID', 'yellow')
  ga_account_id = click.prompt(
    'What the Google Analytics UA ID', default='UA-12345678-9')
  account_id = _check_ga_account_id(ga_account_id)
  identifier = ['GA Client ID', 'User ID']
  _format_heading('GA Join Key type', 'yellow')
  click.echo(
    'The custom dimension join key can be either\n'
    'the GA Client ID (cookie ID) or a unique User ID.\n'
    '--------------------------------------------')
  for i, id in enumerate(identifier):
    click.echo(f'{i + 1}) {id}')
  ind = click.prompt(
    'Enter the index for your join key', type=int) - 1
  join_key = identifier[ind]
  scopes = ['User or Session', 'Hit']
  _format_heading('GA Join Key scope', 'yellow')
  for i, s in enumerate(scopes):
    click.echo(f'{i + 1}) {s}')
  scope_ind = click.prompt(
    f'Enter the index for your {join_key} scope', type=int) - 1
  scope = scopes[scope_ind]
  if join_key == 'User ID':
    if scope == "Hit":
      unnest_where_condition =  """\\r\\n              AND (\\r\\n                SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n              ) IS NOT NULL\\r\\n              AND (\\r\\n                SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n              ) != '0'"""
      key = """(\\r\\n                  SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                  FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n                )"""
      repeat_partition_by_key = """(\\r\\n                                SELECT \\r\\n                                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                                FROM\\r\\n                                    UNNEST(hits) AS h,\\r\\n                                    UNNEST(h.customDimensions) AS cd\\r\\n                            )"""
      repeat_uid_key = """(\\r\\n                          SELECT \\r\\n                            MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                          FROM\\r\\n                            UNNEST(hits) AS h,\\r\\n                            UNNEST(h.customDimensions) AS cd\\r\\n                        )"""
      repeat_unnest_where_condition = """\\r\\n                    AND (\\r\\n                        SELECT \\r\\n                            MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                        FROM\\r\\n                            UNNEST(hits) AS h,\\r\\n                            UNNEST(h.customDimensions) AS cd\\r\\n                        ) IS NOT NULL\\r\\n                    AND (\\r\\n                        SELECT \\r\\n                            MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                        FROM\\r\\n                            UNNEST(hits) AS h,\\r\\n                            UNNEST(h.customDimensions) AS cd\\r\\n                        ) != '0'"""
    if scope == "User or Session":
      unnest_where_condition = """\\r\\n              AND (\\r\\n                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                FROM UNNEST(customDimensions)) IS NOT NULL\\r\\n              AND (\\r\\n                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                FROM UNNEST(customDimensions)) != '0'"""
      key = """(\\r\\n                  SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                  FROM UNNEST(customDimensions)\\r\\n                )"""
      repeat_partition_by_key = """(\\r\\n                                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                                FROM UNNEST(customDimensions))"""
      repeat_uid_key = """(\\r\\n                          SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                          FROM UNNEST(customDimensions)\\r\\n                        )"""
      repeat_unnest_where_condition = """\\r\\n                        AND (\\r\\n                            SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                            FROM UNNEST(customDimensions)) IS NOT NULL\\r\\n                        AND (\\r\\n                            SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                            FROM UNNEST(customDimensions)) != '0'"""
  if join_key == 'GA Client ID':
    unnest_where_condition = ""
    key = "GA.clientId"
    repeat_partition_by_key = "GA.clientId"
    repeat_uid_key = "GA.clientId"
    repeat_unnest_where_condition = ""
  if scope == 'User or Session':
    scope_query = """(\\r\\n                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                FROM UNNEST(customDimensions)\\r\\n            ) AS custom_dimension_userId\\r\\n        FROM `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`\\r\\n        WHERE\\r\\n            _TABLE_SUFFIX BETWEEN FORMAT_DATE(\\r\\n                '%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))\\r\\n            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            AND (\\r\\n                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                FROM UNNEST(customDimensions)) IS NOT NULL\\r\\n            AND (\\r\\n                SELECT MAX(IF(index = {{% CD_USER_ID %}}, value, NULL))\\r\\n                FROM UNNEST(customDimensions)) != '0'\\r\\n        GROUP BY 1, 2\\r\\n    )""".format(bigquery_export_project=bigquery_export_project)
  if scope == 'Hit':
    scope_query = """(\\r\\n                SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n            ) AS custom_dimension_userId\\r\\n        FROM `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`\\r\\n        WHERE\\r\\n            _TABLE_SUFFIX BETWEEN FORMAT_DATE(\\r\\n                '%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))\\r\\n            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            AND (\\r\\n                SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n              ) IS NOT NULL\\r\\n            AND (\\r\\n                SELECT \\r\\n                    MAX(IF(cd.index = {{% CD_USER_ID %}}, cd.value, NULL)) \\r\\n                FROM\\r\\n                    UNNEST(hits) AS h,\\r\\n                    UNNEST(h.customDimensions) AS cd\\r\\n            ) != '0'\\r\\n        GROUP BY 1, 2\\r\\n    )""".format(bigquery_export_project=bigquery_export_project)
  _format_heading('GA Custom Dimension Index - Join Key', 'yellow')
  cd_user_id = click.prompt(
    f'What is the custom dimension index for the {join_key}', type=int)
  _format_heading('GA Custom Dimension Index - Score', 'yellow')
  click.echo(
    f'Create a placeholder custom dimension with\n'
    f'the same scope as the {join_key}\n'
    '--------------------------------------------')
  imported_data = click.prompt(
    'What is the custom dimension index for the imported score', type=int)
  _format_heading('GA Dataset ID', 'yellow')
  msg = (
    'Follow these steps to generate a GA Dataset ID:\n'
    '- Create a Google Analytics custom dataset\n'
    '- Use Query Time import behavior\n'
    '- Share with your GA360 BigQuery Enabled View (at least)\n'
    f'- Set the Key to the {join_key} custom dimension\n'
    '- Set the Imported Data to the custom dimension placeholder\n'
    '--------------------------------------------')
  click.echo(msg)
  ga_dataset_id = click.prompt(
    'What is the Google Analytics Dataset ID', type=str)
  ad_accounts = ['DV360', 'Google Ads', 'Google Ads MCC']
  _format_heading('Audience Destination', 'green')
  click.echo(
    'Choose the destination for your Google Analytics audience.\n'
    'You can always add more destinations later.\n'
    '--------------------------------------------')
  for i, id in enumerate(ad_accounts):
    click.echo(f'{i + 1}) {id}')
  linked_ad_account = click.prompt(
    'Enter the index for the audience destination', type=int) - 1
  linked_ad_account_types = {'0': 'DBM_LINKS', '1': 'ADWORDS_LINKS', '2': 'MCC_LINKS'}
  linked_ad_account_type = linked_ad_account_types[str(linked_ad_account)]
  _format_heading('Audience Destination ID', 'green')
  linked_ad_account_id = click.prompt(
    f'What is the account ID for the {ad_accounts[linked_ad_account]} account', type=str)
  _format_heading('Acknowledgments', 'red')
  cloud_storage = (
    f'Did you create a bucket in Cloud Storage named "{bq_namespace}", yet?')
  click.confirm(cloud_storage, default=True)
  click.echo('--------------------------------------------')
  sa = f'{stage_name.project_id_gae}@appspot.gserviceaccount.com'
  msg = (
      f'Your App Engine default service account is:')
  click.echo(msg)
  click.echo(click.style(sa, fg='bright_cyan'))
  click.echo('--------------------------------------------')
  storage_object_admin = (
    f'Did you give storage object admin permissions to the\n'
    f'App Engine default service account for the\n'
    f'Cloud Storage bucket named "{bq_namespace}", yet?')
  click.confirm(storage_object_admin, default=True)
  edit_permissions = (
    f'\nDid you add edit permissions for the\n'
    f'App Engine default service account to the\n'
    f'Google Analytics property, yet?')
  click.confirm(edit_permissions, default=True)
  if not same_project:
    bq_permissions = (
      f'\nDid you add:\n'
      f'  1) BigQuery Data Viewer &\n'
      f'  2) BigQuery User\n'
      f'permissions for the App Engine default service account\n'
      f'{crmint_project}@appspot.gserviceaccount.com to the\n'
      f'Google Cloud Platform Project "{bigquery_export_project}", yet?')
    click.confirm(bq_permissions, default=True)
  _format_heading('Done >>>> Importing Pipelines', 'green')
  training_params = """
    "params": [
        {{
          "type": "text",
          "name": "BQ_PROJECT",
          "value": "{bq_project_id}"
        }},
        {{
          "type": "text",
          "name": "BQ_DATASET",
          "value": "{bq_dataset_id}"
        }},
        {{
          "type": "text",
          "name": "BQ_NAMESPACE",
          "value": "{bq_namespace}"
        }},
        {{
          "type": "text",
          "name": "CD_USER_ID",
          "value": "{cd_user_id}"
        }},
        {{
          "type": "text",
          "name": "BQ_DATASET_LOCATION",
          "value": "{bq_dataset_location}"
        }}""".format(
      bq_project_id=crmint_project,
      bq_dataset_id=bq_dataset_id,
      bq_namespace=bq_namespace,
      bq_dataset_location=bq_dataset_location,
      cd_user_id=cd_user_id)
  prediction_params = """
    "params": [
      {{
        "type": "text",
        "name": "BQ_PROJECT",
        "value": "{bq_project_id}"
      }},
      {{
        "type": "text",
        "name": "BQ_DATASET",
        "value": "{bq_dataset_id}"
      }},
      {{
        "type": "text",
        "name": "BQ_NAMESPACE",
        "value": "{bq_namespace}"
      }},
      {{
        "type": "text",
        "name": "GCS_BUCKET",
        "value": "{bq_namespace}"
      }},
      {{
        "type": "text",
        "name": "CD_USER_ID",
        "value": "{cd_user_id}"
      }},
      {{
        "type": "text",
        "name": "CD_SCORE",
        "value": "{cd_score}"
      }},
      {{
        "type": "text",
        "name": "GA_PROPERTY_ID",
        "value": "{formatted_ga_property_id}"
      }},
      {{
        "type": "text",
        "name": "GA_ACCOUNT_ID",
        "value": "{ga_account_id}"
      }},
      {{
        "type": "text",
        "name": "GA_DATASET_ID",
        "value": "{ga_dataset_id}"
      }},
      {{
        "type": "text",
        "name": "BQ_DATASET_LOCATION",
        "value": "{bq_dataset_location}"
      }}""".format(
        bq_project_id=crmint_project,
        bq_dataset_id=bq_dataset_id,
        bq_namespace=bq_namespace,
        bq_dataset_location=bq_dataset_location,
        cd_user_id=cd_user_id,
        cd_score=imported_data,
        ga_account_id=account_id,
        formatted_ga_property_id=ga_account_id,
        ga_dataset_id=ga_dataset_id)
  if objective == 'Repeat Purchase Propensity':
    visitors_labeled = """converters AS (\\r\\n            SELECT {cid}, event_session, event_date \\r\\n            FROM (\\r\\n                SELECT \\r\\n                    {cid}, \\r\\n                    visitStartTime AS event_session, \\r\\n                    date AS event_date,\\r\\n                    RANK() OVER (PARTITION BY {cid} ORDER BY visitStartTime ASC) \\r\\n                        AS unique_purchase\\r\\n                FROM\\r\\n                    `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`\\r\\n                WHERE \\r\\n                    _TABLE_SUFFIX BETWEEN\\r\\n                        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                        AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n                    AND totals.transactions >= 1\\r\\n                GROUP BY {cid}, event_session, event_date\\r\\n            )\\r\\n            WHERE unique_purchase = 2\\r\\n        ),\\r\\n        non_converters AS (\\r\\n            SELECT\\r\\n                {cid},\\r\\n                0 AS event_session,\\r\\n                '0' AS event_date\\r\\n            FROM\\r\\n                `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`\\r\\n            WHERE \\r\\n                _TABLE_SUFFIX BETWEEN\\r\\n                    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n                AND {cid} NOT IN (SELECT {cid} FROM converters)\\r\\n            GROUP BY {cid}, event_session, event_date\\r\\n        ),\\r\\n        combined AS (\\r\\n            SELECT {cid}, event_session, event_date\\r\\n            FROM converters  \\r\\n            UNION ALL\\r\\n            SELECT {cid}, event_session, event_date\\r\\n            FROM non_converters \\r\\n            GROUP BY {cid}, event_session, event_date\\r\\n        ),\\r\\n        visitors_labeled AS ( \\r\\n            SELECT\\r\\n                {cid}, \\r\\n                CASE \\r\\n                    WHEN event_session > 0\\r\\n                    THEN event_session END AS event_session, \\r\\n                CASE \\r\\n                    WHEN event_date != '0'\\r\\n                    THEN event_date END AS event_date, \\r\\n                CASE \\r\\n                    WHEN event_session > 0\\r\\n                    THEN 1 ELSE 0 END AS label\\r\\n            FROM \\r\\n                combined\\r\\n            GROUP BY\\r\\n                {cid}, event_session, event_date, label\\r\\n        );"""
  if objective == 'Purchase Propensity':
    visitors_labeled = """visitors_labeled AS ( \\r\\n            SELECT\\r\\n              {cid}, \\r\\n              MIN(\\r\\n                CASE \\r\\n                  WHEN totals.transactions >= 1 \\r\\n                  THEN visitStartTime END) AS event_session, \\r\\n              MIN(\\r\\n                CASE \\r\\n                  WHEN totals.transactions >= 1 \\r\\n                  THEN date END) AS event_date, \\r\\n              MAX(\\r\\n                CASE \\r\\n                  WHEN totals.transactions >= 1 \\r\\n                  THEN 1 \\r\\n                  ELSE 0 END) AS label\\r\\n            FROM \\r\\n             `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            GROUP BY \\r\\n              {cid}\\r\\n          )"""
  if objective == 'Event Propensity':
    event_params = """,
        {{
            "type": "text",
            "name": "EVENT_CATEGORY",
            "value": "{event_category}"
        }},
        {{
            "type": "text",
            "name": "EVENT_ACTION",
            "value": "{event_action}"
        }},
        {{
            "type": "text",
            "name": "EVENT_LABEL",
            "value": "{event_label}"
        }}""".format(
          event_category=event_category,
          event_action=event_action,
          event_label=event_label)
    training_params += event_params
    prediction_params += event_params
    visitors_labeled = """visitors_labeled AS (\\r\\n            SELECT\\r\\n              {cid},\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.eventInfo.eventCategory, '(?i){{% EVENT_CATEGORY %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventAction, '(?i){{% EVENT_ACTION %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventLabel, '(?i){{% EVENT_LABEL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN visitStartTime\\r\\n                  END\\r\\n              ) AS event_session,\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.eventInfo.eventCategory, '(?i){{% EVENT_CATEGORY %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventAction, '(?i){{% EVENT_ACTION %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventLabel, '(?i){{% EVENT_LABEL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN date\\r\\n                  END\\r\\n              ) AS event_date,\\r\\n              MAX(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.eventInfo.eventCategory, '(?i){{% EVENT_CATEGORY %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventAction, '(?i){{% EVENT_ACTION %}}')\\r\\n                      AND REGEXP_CONTAINS(hits.eventInfo.eventLabel, '(?i){{% EVENT_LABEL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN 1\\r\\n                    ELSE 0\\r\\n                  END\\r\\n              ) AS label\\r\\n            FROM\\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`,\\r\\n              UNNEST(hits) AS hits\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            GROUP BY\\r\\n              {cid}\\r\\n          )"""
  if objective == 'Destination Propensity':
    destination_params = """,
      {{
        "type": "text",
        "name": "DESTINATION_URL",
        "value": "{destination_url}"
      }}""".format(destination_url=destination_url)
    training_params += destination_params
    prediction_params += destination_params
    visitors_labeled = """visitors_labeled AS ( \\r\\n            SELECT\\r\\n              {cid},\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.page.pagePath, '(?i){{% DESTINATION_URL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN visitStartTime\\r\\n                  END\\r\\n              ) AS event_session,\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.page.pagePath, '(?i){{% DESTINATION_URL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN date\\r\\n                  END\\r\\n              ) AS event_date,\\r\\n              MAX(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS(hits.page.pagePath, '(?i){{% DESTINATION_URL %}}'),\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN 1\\r\\n                    ELSE 0\\r\\n                  END\\r\\n              ) AS label\\r\\n            FROM\\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`,\\r\\n              UNNEST(hits) AS hits\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            GROUP BY\\r\\n              {cid}\\r\\n          )"""
  if objective == 'Product Propensity':
    product_params = """,
      {{
          "type": "text",
          "name": "PRODUCT",
          "value": "{product}"
      }}""".format(product=product)
    training_params += product_params
    prediction_params += product_params
    visitors_labeled = """visitors_labeled AS (\\r\\n            SELECT\\r\\n              {cid},\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS({product_dimension}, '(?i)^{{% PRODUCT %}}$')\\r\\n                      AND h.eCommerceAction.action_type = '6',\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN visitStartTime\\r\\n                  END\\r\\n              ) AS event_session,\\r\\n              MIN(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS({product_dimension}, '(?i)^{{% PRODUCT %}}$')\\r\\n                      AND h.eCommerceAction.action_type = '6',\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN date\\r\\n                  END\\r\\n              ) AS event_date,\\r\\n              MAX(\\r\\n                CASE\\r\\n                  WHEN\\r\\n                    IF(\\r\\n                      REGEXP_CONTAINS({product_dimension}, '(?i)^{{% PRODUCT %}}$')\\r\\n                      AND h.eCommerceAction.action_type = '6',\\r\\n                      TRUE,\\r\\n                      FALSE) IS TRUE\\r\\n                    THEN 1\\r\\n                    ELSE 0\\r\\n                  END\\r\\n              ) AS label\\r\\n            FROM\\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*`,\\r\\n              UNNEST(hits) AS h,\\r\\n              UNNEST(h.product) AS product_propensity\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n            GROUP BY\\r\\n              {cid}\\r\\n          )"""
  if objective == 'Custom Dimension Propensity':
    custom_dimension_params = """,
      {{
          "type": "text",
          "name": "CUSTOM_DIMENSION_INDEX",
          "value": "{custom_dimension_index}"
      }},
      {{
          "type": "text",
          "name": "CUSTOM_DIMENSION_VALUE",
          "value": "{custom_dimension_value}"
      }}""".format(
        custom_dimension_index=custom_dimension_index,
        custom_dimension_value=custom_dimension_value)
    training_params += custom_dimension_params
    prediction_params += custom_dimension_params
    visitors_labeled = """visitors_labeled AS (\\r\\n          SELECT\\r\\n              {cid}, \\r\\n              MIN(\\r\\n                  CASE \\r\\n                      WHEN \\r\\n                        cd.index = {{% CUSTOM_DIMENSION_INDEX }%}\\r\\n                        AND REGEXP_CONTAINS(\\r\\n                            cd.value, '(?i){{% CUSTOM_DIMENSION_VALUE %}}')\\r\\n                      THEN visitStartTime END) AS event_session, \\r\\n              MIN(\\r\\n                  CASE \\r\\n                      WHEN \\r\\n                        cd.index = {{% CUSTOM_DIMENSION_INDEX %}}\\r\\n                        AND REGEXP_CONTAINS(\\r\\n                            cd.value, '(?i){{% CUSTOM_DIMENSION_VALUE %}}')\\r\\n                      THEN date END) AS event_date, \\r\\n              MAX(\\r\\n                  CASE \\r\\n                      WHEN\\r\\n                        cd.index = {{% CUSTOM_DIMENSION_INDEX %}}\\r\\n                        AND REGEXP_CONTAINS(\\r\\n                            cd.value, '(?i){{% CUSTOM_DIMENSION_VALUE %}}')\\r\\n                      THEN 1 \\r\\n                      ELSE 0 END) AS label\\r\\n          FROM \\r\\n            `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA,\\r\\n            {cd_scope_query}\\r\\n          WHERE \\r\\n            _TABLE_SUFFIX BETWEEN\\r\\n              FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n          GROUP BY \\r\\n              {cid}\\r\\n          )"""
  prediction_pipeline_name = f'{objective} Prediction Pipeline'
  training_pipeline_name = f'{objective} Training Pipeline'
  prediction_name = f'{objective} Prediction'
  training_name = f'{objective} Training'
  visitors_labeled = visitors_labeled.format(
    cid=cid,
    crmint_project=crmint_project,
    bigquery_export_project=bigquery_export_project,
    product_dimension=product_dimension,
    cd_scope_query=cd_scope_query)
  PREDICTION_QUERY = """SELECT uid AS {cid}, predicted_will_convert_later\\r\\nFROM\\r\\n  ml.predict(\\r\\n    MODEL `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_model`,\\r\\n    (\\r\\n      WITH \\r\\n          {visitors_labeled},\\r\\n          visitor_region AS (\\r\\n            SELECT\\r\\n              GA.{cid}, \\r\\n              MAX(geoNetwork.region) AS region\\r\\n            FROM \\r\\n             `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL)\\r\\n            GROUP BY \\r\\n              {cid}\\r\\n          ),\\r\\n          visitor_day_page_map AS (\\r\\n            SELECT \\r\\n              GA.{cid},\\r\\n              EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) AS day,\\r\\n              SUM(totals.pageviews) AS pages_viewed\\r\\n            FROM\\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n              FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n              AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL)\\r\\n              GROUP BY 1, 2\\r\\n          ),\\r\\n          visitor_common_day AS (\\r\\n            SELECT \\r\\n              {cid},\\r\\n              /* In the event of a tie, pick any of the top dates. */\\r\\n              CASE \\r\\n                WHEN ANY_VALUE(day) = 1 THEN 'Sunday'\\r\\n                WHEN ANY_VALUE(day) = 2 THEN 'Monday'\\r\\n                WHEN ANY_VALUE(day) = 3 THEN 'Tuesday'\\r\\n                WHEN ANY_VALUE(day) = 4 THEN 'Wednesday'\\r\\n                WHEN ANY_VALUE(day) = 5 THEN 'Thursday'\\r\\n                WHEN ANY_VALUE(day) = 6 THEN 'Friday'\\r\\n                WHEN ANY_VALUE(day) = 7 THEN 'Saturday' \\r\\n              END AS day\\r\\n            FROM \\r\\n              visitor_day_page_map AS day_page_map\\r\\n            WHERE day_page_map.pages_viewed = (\\r\\n              SELECT MAX(pages_viewed)\\r\\n              FROM visitor_day_page_map AS day_map\\r\\n              WHERE day_page_map.{cid} = day_map.{cid})\\r\\n            GROUP BY 1\\r\\n          ),\\r\\n          users_sessions AS (\\r\\n            SELECT \\r\\n                {key} AS uid,\\r\\n                IFNULL(MAX(label), 0) AS will_convert_later,\\r\\n                MAX(Visitor_region.region) AS visited_region,\\r\\n                MAX(Visitor_common_day.day) AS visited_dow,\\r\\n                COUNT(distinct visitId) AS total_sessions,\\r\\n                SUM(totals.pageviews) AS pageviews,\\r\\n                COUNT(totals.bounces) / COUNT(distinct visitId) AS bounce_rate,\\r\\n                SUM(totals.pageviews) / COUNT(distinct visitId) AS avg_session_depth,\\r\\n                MAX(CASE WHEN device.isMobile IS TRUE THEN 1 ELSE 0 END) AS mobile,\\r\\n                MAX(CASE WHEN device.browser = 'Chrome' THEN 1 ELSE 0 END) AS chrome,\\r\\n                MAX(CASE WHEN device.browser LIKE  '%Safari%' THEN 1 ELSE 0 END) AS safari,\\r\\n                MAX(\\r\\n                  CASE WHEN device.browser <> 'Chrome' AND device.browser NOT LIKE '%Safari%' THEN 1 ELSE 0 END) AS browser_other,\\r\\n                SUM(CASE WHEN trafficSource.medium = '(none)' THEN 1 ELSE 0 END) AS visits_traffic_source_none,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'organic' THEN 1 ELSE 0 END) AS visits_traffic_source_organic,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'cpc' THEN 1 ELSE 0 END) AS visits_traffic_source_cpc,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'cpm' THEN 1 ELSE 0 END) AS visits_traffic_source_cpm,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'affiliate' THEN 1 ELSE 0 END) AS visits_traffic_source_affiliate,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'referral' THEN 1 ELSE 0 END) AS visits_traffic_source_referral,\\r\\n                COUNT(distinct geoNetwork.region) AS distinct_regions,\\r\\n                COUNT(distinct EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date))) AS num_diff_days_visited\\r\\n            FROM \\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            LEFT JOIN visitor_region AS Visitor_region\\r\\n              ON GA.{cid} = Visitor_region.{cid}\\r\\n            LEFT JOIN visitor_common_day AS Visitor_common_day\\r\\n              ON GA.{cid} = Visitor_common_day.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL){unnest_where_condition}\\r\\n            GROUP BY \\r\\n              1\\r\\n          )\\r\\n        SELECT \\r\\n          * \\r\\n        FROM \\r\\n          users_sessions\\r\\n        WHERE \\r\\n          bounce_rate < 1.0\\r\\n    )\\r\\n  );"""
  TRAINING_QUERY = """#standardSQL\\r\\n{create_dataset}CREATE OR REPLACE MODEL `{crmint_project}.{{% BQ_DATASET %}}.{{% BQ_NAMESPACE %}}_model`\\r\\n    OPTIONS ({model_options})\\r\\nAS (\\r\\n    WITH \\r\\n        {visitors_labeled},\\r\\n          visitor_region AS (\\r\\n            SELECT\\r\\n              GA.{cid}, \\r\\n              MAX(geoNetwork.region) AS region\\r\\n            FROM \\r\\n             `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL)\\r\\n            GROUP BY \\r\\n              {cid}\\r\\n          ),\\r\\n          visitor_day_page_map AS (\\r\\n            SELECT \\r\\n              GA.{cid},\\r\\n              EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) AS day,\\r\\n              SUM(totals.pageviews) AS pages_viewed\\r\\n            FROM\\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n              FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n              AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL)\\r\\n              GROUP BY 1, 2\\r\\n          ),\\r\\n          visitor_common_day AS (\\r\\n            SELECT \\r\\n              {cid},\\r\\n              /* In the event of a tie, pick any of the top dates. */\\r\\n              CASE \\r\\n                WHEN ANY_VALUE(day) = 1 THEN 'Sunday'\\r\\n                WHEN ANY_VALUE(day) = 2 THEN 'Monday'\\r\\n                WHEN ANY_VALUE(day) = 3 THEN 'Tuesday'\\r\\n                WHEN ANY_VALUE(day) = 4 THEN 'Wednesday'\\r\\n                WHEN ANY_VALUE(day) = 5 THEN 'Thursday'\\r\\n                WHEN ANY_VALUE(day) = 6 THEN 'Friday'\\r\\n                WHEN ANY_VALUE(day) = 7 THEN 'Saturday' \\r\\n              END AS day\\r\\n            FROM \\r\\n              visitor_day_page_map AS day_page_map\\r\\n            WHERE day_page_map.pages_viewed = (\\r\\n              SELECT MAX(pages_viewed)\\r\\n              FROM visitor_day_page_map AS day_map\\r\\n              WHERE day_page_map.{cid} = day_map.{cid})\\r\\n            GROUP BY 1\\r\\n          ),\\r\\n          users_sessions AS (\\r\\n            SELECT \\r\\n                {key} AS uid,\\r\\n                IFNULL(MAX(label), 0) AS will_convert_later,\\r\\n                MAX(Visitor_region.region) AS visited_region,\\r\\n                MAX(Visitor_common_day.day) AS visited_dow,\\r\\n                COUNT(distinct visitId) AS total_sessions,\\r\\n                SUM(totals.pageviews) AS pageviews,\\r\\n                COUNT(totals.bounces) / COUNT(distinct visitId) AS bounce_rate,\\r\\n                SUM(totals.pageviews) / COUNT(distinct visitId) AS avg_session_depth,\\r\\n                MAX(CASE WHEN device.isMobile IS TRUE THEN 1 ELSE 0 END) AS mobile,\\r\\n                MAX(CASE WHEN device.browser = 'Chrome' THEN 1 ELSE 0 END) AS chrome,\\r\\n                MAX(CASE WHEN device.browser LIKE  '%Safari%' THEN 1 ELSE 0 END) AS safari,\\r\\n                MAX(\\r\\n                  CASE WHEN device.browser <> 'Chrome' AND device.browser NOT LIKE '%Safari%' THEN 1 ELSE 0 END) AS browser_other,\\r\\n                SUM(CASE WHEN trafficSource.medium = '(none)' THEN 1 ELSE 0 END) AS visits_traffic_source_none,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'organic' THEN 1 ELSE 0 END) AS visits_traffic_source_organic,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'cpc' THEN 1 ELSE 0 END) AS visits_traffic_source_cpc,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'cpm' THEN 1 ELSE 0 END) AS visits_traffic_source_cpm,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'affiliate' THEN 1 ELSE 0 END) AS visits_traffic_source_affiliate,\\r\\n                SUM(CASE WHEN trafficSource.medium = 'referral' THEN 1 ELSE 0 END) AS visits_traffic_source_referral,\\r\\n                COUNT(distinct geoNetwork.region) AS distinct_regions,\\r\\n                COUNT(distinct EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date))) AS num_diff_days_visited\\r\\n            FROM \\r\\n              `{bigquery_export_project}.{{% BQ_DATASET %}}.ga_sessions_*` AS GA\\r\\n            LEFT JOIN visitors_labeled AS Labels\\r\\n              ON GA.{cid} = Labels.{cid}\\r\\n            LEFT JOIN visitor_region AS Visitor_region\\r\\n              ON GA.{cid} = Visitor_region.{cid}\\r\\n            LEFT JOIN visitor_common_day AS Visitor_common_day\\r\\n              ON GA.{cid} = Visitor_common_day.{cid}\\r\\n            WHERE \\r\\n              _TABLE_SUFFIX BETWEEN\\r\\n                FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH))\\r\\n                AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())\\r\\n              AND (\\r\\n                GA.visitStartTime < IFNULL(event_session, 0)\\r\\n                OR event_session IS NULL){unnest_where_condition}\\r\\n            GROUP BY \\r\\n              1\\r\\n          )\\r\\n        SELECT \\r\\n          * \\r\\n          EXCEPT (uid)\\r\\n        FROM \\r\\n          users_sessions\\r\\n        WHERE \\r\\n          bounce_rate < 1.0 \\r\\n);"""
  training_query = TRAINING_QUERY.format(
      cid=cid,
      create_dataset=create_dataset,
      crmint_project=crmint_project,
      bigquery_export_project=bigquery_export_project,
      key=key,
      model_options=model_options,
      unnest_where_condition=unnest_where_condition,
      visitors_labeled=visitors_labeled)
  prediction_query = PREDICTION_QUERY.format(
    cid=cid,
    create_dataset=create_dataset,
    crmint_project=crmint_project,
    bigquery_export_project=bigquery_export_project,
    key=key,
    model_options=model_options,
    unnest_where_condition=unnest_where_condition,
    visitors_labeled=visitors_labeled)
  training = UA_TRAINING_PIPELINE.format(
     training_params=training_params,
     training_query=training_query,
     crmint_project=crmint_project,
     training_name=training_name,
     training_pipeline_name=training_pipeline_name)
  prediction = UA_PREDICTION_PIPELINE.format(
    prediction_query=prediction_query,
    prediction_params=prediction_params,
    crmint_project=crmint_project,
    prediction_pipeline_name=prediction_pipeline_name,
    linked_account_id=linked_ad_account_id,
    linked_account_type=linked_ad_account_type,
    cid=cid,
    scope_query=scope_query,
    model_objective=objective)
  training_filename = 'training_pipeline.json'
  prediction_filename = 'prediction_pipeline.json'
  training_filepath = os.path.join(constants.STAGE_DIR, training_filename)
  with open(training_filepath, 'w+') as fp:
    fp.write(training)
  prediction_filepath = os.path.join(constants.STAGE_DIR, prediction_filename)
  with open(prediction_filepath, 'w+') as fp:
    fp.write(prediction)
  return training_filepath, prediction_filepath
