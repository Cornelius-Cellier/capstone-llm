import argparse
import logging
from pyspark.sql import SparkSession
import boto3
from capstonellm.common.spark import ClosableSparkSession
from pyspark.sql.functions import col, explode

logger = logging.getLogger(__name__)

def save_row_to_s3(rows, tag):
    s3 = boto3.client('s3', region_name='us-east-1')

    bucket_name = "dataminded-academy-capstone-llm-data-us"
    
    for row in rows:
        row_data = row.asDict()
        json_data = json.dumps(row_data)
        file_name = f"cleaned/{tag}/{row['id']}.json" 
        s3.put_object(Body=json_data, Bucket=bucket_name, Key=file_name)

def save_df_to_s3(df, tag):
    df.repartition(30)  

    df.foreachPartition(lambda rows: save_row_to_s3(rows, tag))

def save_df_to_s3(df, tag):
    df.repartition(100)  
    df.foreachPartition(lambda rows: save_row_to_s3(rows, tag))

def clean(spark: SparkSession, environment: str, tag: str):
    questions_json_path = "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json"
    answers_json_path = "s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json"

    nested_questions = spark.read.json(questions_json_path, multiLine=True)
    nested_answers = spark.read.json(answers_json_path, multiLine=True)

    flat_questions = nested_questions.select(explode(col("items")).alias("question")).select(
        col("question.question_id").alias("question_id"),
        col("question.title").alias("title"),
        col("question.body").alias("body")
    )

    flat_answers = nested_answers.select(explode(col("items")).alias("answers")).select(
        col("answers.answer_id").alias("answer_id"),
        col("answers.question_id").alias("question_id"),
        col("answers.body").alias("body")
    )

    df = flat_answers.join(flat_questions, flat_answers.question_id == flat_questions.question_id, "inner")


    pass

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
