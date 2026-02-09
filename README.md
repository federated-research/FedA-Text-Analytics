# TRE Text Analytics Pipeline
This repository is for in-development/experimental code relating to the TRE text analytics and NLP tasks.

## Test Database Installation
If there is no Postgresql database currently installed, run the docker command located in "create_postgres_container.sh".

Edit the database installation script (mimic_4_utility_scripts/import_mimic_discharge.py) to ensure your local athena vocab file path, MIMIC-IV-Note discharge csv file path and Postgres connection details are correct.

Once executed successfully, omop_cdm should appear as a schema under the postgres DB (configurable).

## Demo Setup
This application utilizes CogStack ModelServe for the serving and execution of various NLP statistical models. While there are different ways of deploying CogStack ModelServe (see https://github.com/CogStack/CogStack-ModelServe), this demo is configured to query ModelServe in batches from a queryable Postgresql dataset. Configuration settings can be edited within the "config.env" file.

The federated analysis executable image will utilise the "run.py" script to execute the provided command arguments, however this can alternatively be run as a seperate server using "server.py" instead.

### Example analysis run
Executing as a single run, using arguments that would be specified within a TES message:
```
python run.py --model "medcat_snomed" 
--query "SELECT * FROM note LIMIT 50" 
--batch_size 50 
--output_csv "/home/msztr1/Documents/TREDataOutput/ner_results.csv"
```
Available arguments:

| Argument | Options | Description |
| - | - | - |
| model | medcat_snomed,medcat_umls,medcat_icd10,medcat_opcs4,medcat_deid,anoncat,transformers_deid,huggingface_ner,huggingface_llm| Model type strings recognized by CogStack ModelServe |
| query | string | SQL query to target specific data to analyze |
| batch_size | int | Batch size to query & store in memory for NER processing, larger sized will require more memory. Default 50. |
| output_csv | string | Path to output the NER results. |

