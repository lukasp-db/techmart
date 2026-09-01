-- ai_query text-fill for the AI schema's marquee tables. Runs on a serverless
-- SQL warehouse (bundle-provisioned). CREATE OR REPLACE TABLE ... AS SELECT (RTAS)
-- does NOT permit an explicit column list, so table/column comments (the Genie
-- contract) are applied with COMMENT ON TABLE + ALTER COLUMN ... COMMENT afterward.

-- ============================ product_review ============================
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review')
AS SELECT
  review_id, product_sk, customer_sk, date_sk, rating,
  ai_query(:llm_endpoint, title_prompt) AS review_title,
  ai_query(:llm_endpoint, prompt)       AS review_text,
  verified_purchase, helpful_votes
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._product_review_staging');

COMMENT ON TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') IS 'one row per product review';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN review_id COMMENT 'Surrogate key';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN product_sk COMMENT 'Product FK (dim_product)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN customer_sk COMMENT 'Customer FK (dim_customer)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN date_sk COMMENT 'Review date FK (dim_date)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN rating COMMENT 'Star rating 1-5';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN review_title COMMENT 'LLM-generated review title';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN review_text COMMENT 'LLM-generated review body';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN verified_purchase COMMENT 'Tied to a real purchase';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.product_review') ALTER COLUMN helpful_votes COMMENT 'Helpful-vote count';

-- ============================ service_case ============================
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case')
AS SELECT
  case_id, customer_sk, product_sk, store_sk, date_sk, case_type, channel, status,
  ai_query(:llm_endpoint, notes_prompt) AS case_notes,
  CASE WHEN resolution_prompt IS NULL THEN NULL
       ELSE ai_query(:llm_endpoint, resolution_prompt) END AS resolution_notes,
  csat_score
FROM IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai._service_case_staging');

COMMENT ON TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') IS 'one row per service case';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN case_id COMMENT 'Surrogate key';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN customer_sk COMMENT 'Customer FK (dim_customer)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN product_sk COMMENT 'Product FK (dim_product)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN store_sk COMMENT 'Store FK (dim_store)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN date_sk COMMENT 'Case open date FK (dim_date)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN case_type COMMENT 'Repair/Warranty/Support';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN channel COMMENT 'Phone/In-Store/Online';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN status COMMENT 'Open/In-Progress/Resolved/Closed';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN case_notes COMMENT 'LLM-generated case notes';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN resolution_notes COMMENT 'LLM-generated resolution (null if unresolved)';
ALTER TABLE IDENTIFIER(:catalog || '.' || :schema_prefix || 'ai.service_case') ALTER COLUMN csat_score COMMENT 'Customer satisfaction 1-5';
