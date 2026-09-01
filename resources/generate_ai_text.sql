-- Fill review text from the staging table.
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') AS
SELECT
  review_id, product_sk, customer_sk, date_sk, rating,
  ai_query(:llm_endpoint, title_prompt) AS review_title,
  ai_query(:llm_endpoint, prompt)       AS review_text,
  verified_purchase, helpful_votes
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._product_review_staging');

-- Fill service-case text from the staging table (resolution only when resolved/closed).
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') AS
SELECT
  case_id, customer_sk, product_sk, store_sk, date_sk, case_type, channel, status,
  ai_query(:llm_endpoint, notes_prompt) AS case_notes,
  CASE WHEN resolution_prompt IS NULL THEN NULL
       ELSE ai_query(:llm_endpoint, resolution_prompt) END AS resolution_notes,
  csat_score
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._service_case_staging');
