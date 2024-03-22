CREATE DATABASE ml_data;

USE ml_data; 

SET @db_list = '["ml_data"]';

SET @ext_tables = '[
{
  "db_name": "ml_data",
  "tables": [{
    "table_name": "bank_marketing",
    "dialect": {
        "format": "csv",
         "field_delimiter": ";",
          "record_delimiter": "\\n",
          "has_header": true,
          "skip_rows": 0
      },
    "file": [{
      "par": "<PARL_URL>"
        }]
   }]
}
]';

SET @options = JSON_OBJECT('external_tables', CAST(@ext_tables AS JSON));

CALL sys.heatwave_load(@db_list, @options);

DESCRIBE ml_data.bank_marketing;

-- Train the model
CALL sys.ML_TRAIN('ml_data.bank_marketing', 'y', JSON_OBJECT('task', 'classification'), @model_bank);

-- Load the model into HeatWave
CALL sys.ML_MODEL_LOAD(@model_bank, NULL);

-- Score the model on the test data
CALL sys.ML_SCORE('ml_data.bank_marketing', 'y', @model_bank, 'accurarcy', @score_bank, null);

-- Print the score
SELECT @score_bank;

CREATE TABLE bank_marketing_test
        AS SELECT * from bank_marketing_test LIMIT 20;
    
ALTER TABLE bank_marketing_test SECONDARY_LOAD;

CALL sys.ML_PREDICT_TABLE('ml_data.bank_marketing_test', @bank_model, 
        'ml_data.bank_marketing_predictions', NULL);

CALL sys.ML_EXPLAIN('ml_data.bank_marketing_test', 'y', @bank_model, 
        JSON_OBJECT('prediction_explainer', 'permutation_importance'));

CALL sys.ML_EXPLAIN_TABLE('ml_data.bank_marketing_test', @bank_model, 
        'ml_data.bank_marketing_lakehouse_test_explanations', 
        JSON_OBJECT('prediction_explainer', 'permutation_importance'));

SET @row_input = JSON_OBJECT(
    'age', bank_marketing_test.age,
    'job', bank_marketing_test.job,
    'marital', bank_marketing_test.marital,
    'education', bank_marketing_test.education,
    'default1', bank_marketing_test.default1,
    'balance', bank_marketing_test.balance,
    'housing', bank_marketing_test.housing,
    'loan', bank_marketing_test.loan,
    'contact', bank_marketing_test.contact,
    'day', bank_marketing_test.day,
    'month', bank_marketing_test.month,
    'duration', bank_marketing_test.duration,
    'campaign', bank_marketing_test.campaign,
    'pdays', bank_marketing_test.pdays,
    'previous', bank_marketing_test.previous,
    'poutcome', bank_marketing_test.poutcome);

SELECT sys.ML_PREDICT_ROW(@row_input, @bank_model, NULL)
    FROM bank_marketing_test LIMIT 4;

SELECT sys.ML_EXPLAIN_ROW(@row_input, @bank_model,
        JSON_OBJECT('prediction_explainer', 'permutation_importance'))
        FROM bank_marketing_test LIMIT 4;