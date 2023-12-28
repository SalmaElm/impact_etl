CREATE OR REPLACE STAGE PROD_DB.GROWTH.impact_stg
  URL = 's3://impact-performance-data/'
  CREDENTIALS = (AWS_KEY_ID = '{}' AWS_SECRET_KEY = '{}')