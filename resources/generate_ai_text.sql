CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') (
  review_id COMMENT 'Surrogate key',
  product_sk COMMENT 'Product FK (dim_product)',
  customer_sk COMMENT 'Customer FK (dim_customer)',
  date_sk COMMENT 'Review date FK (dim_date)',
  rating COMMENT 'Star rating 1-5',
  review_title COMMENT 'LLM-generated review title',
  review_text COMMENT 'LLM-generated review body',
  verified_purchase COMMENT 'Tied to a real purchase',
  helpful_votes COMMENT 'Helpful-vote count'
) COMMENT 'one row per product review'
AS SELECT
  review_id, product_sk, customer_sk, date_sk, rating,
  ai_query(:llm_endpoint, title_prompt) AS review_title,
  ai_query(:llm_endpoint, prompt)       AS review_text,
  verified_purchase, helpful_votes
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._product_review_staging');

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') (
  case_id COMMENT 'Surrogate key',
  customer_sk COMMENT 'Customer FK (dim_customer)',
  product_sk COMMENT 'Product FK (dim_product)',
  store_sk COMMENT 'Store FK (dim_store)',
  date_sk COMMENT 'Case open date FK (dim_date)',
  case_type COMMENT 'Repair/Warranty/Support',
  channel COMMENT 'Phone/In-Store/Online',
  status COMMENT 'Open/In-Progress/Resolved/Closed',
  case_notes COMMENT 'LLM-generated case notes',
  resolution_notes COMMENT 'LLM-generated resolution (null if unresolved)',
  csat_score COMMENT 'Customer satisfaction 1-5'
) COMMENT 'one row per service case'
AS SELECT
  case_id, customer_sk, product_sk, store_sk, date_sk, case_type, channel, status,
  ai_query(:llm_endpoint, notes_prompt) AS case_notes,
  CASE WHEN resolution_prompt IS NULL THEN NULL
       ELSE ai_query(:llm_endpoint, resolution_prompt) END AS resolution_notes,
  csat_score
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._service_case_staging');
