from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def check_gcs_prefix_exists(ds):
    from google.cloud import storage
    from google.api_core.exceptions import NotFound

    bucket_name = "jacob-wiki-bucket"
    prefix = f"bigque/parquet/ko/date={ds}/"

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        if blobs:
            print(f"GCS 경로 존재: {prefix}")
            return 'check_bq_partition'
        else:
            print(f"GCS에 파일 없음: {prefix}")
            return 'skip_load'

    except NotFound:
        print(f"버킷 없음: {bucket_name}")
        return 'skip_load'
    except Exception as e:
        print(f"예외 발생: {e}")
        return 'skip_load'

def check_bq_partition_exists(ds):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    client = bigquery.Client()
    try:
        query = f"""
        SELECT 1
        FROM load.wiki
        WHERE date = '{ds}'
        LIMIT 1
        """
        query_job = client.query(query)
        rows = query_job.result()

        if any(True for _ in rows):
            print("데이터 있음 → skip")
            return 'skip_load'
        else:
            print("테이블 있음, 데이터 없음 → load")
            return 'load_parquet_partition'

    except NotFound:
        print("테이블 'load.wiki' 없음 → load")
        return 'load_parquet_partition'
    except Exception as e:
        print(f"BQ 쿼리 실패: {e}")
        return 'load_parquet_partition'

with DAG(
    dag_id='bq_load_and_partition_check',
    description='Check GCS prefix + BQ partition and load if needed',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 4, 8),
    catchup=True,
    max_active_runs=5,
    concurrency=10,
    tags=["bq", "gcs", "wiki", "partition"],
) as dag:

    # GCS 경로 존재 여부 확인
    check_gcs = PythonVirtualenvOperator(
        task_id='check_gcs_prefix',
        python_callable=check_gcs_prefix_exists,
        op_kwargs={"ds": "{{ ds }}"},
        requirements=["google-cloud-storage"],
        system_site_packages=False,
    )
    
    def branch_on_gcs(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='check_gcs_prefix')
        print(f"[BRANCH] GCS 분기 결과: {result}")
        return result

    branch_after_gcs = BranchPythonOperator(
        task_id='branch_after_gcs_check',
        python_callable=branch_on_gcs,
        provide_context=True,
    )
    
    # GCS 경로에 있으면 → BQ 파티션 확인
    check_bq = PythonVirtualenvOperator(
        task_id='check_bq_partition',
        python_callable=check_bq_partition_exists,
        op_kwargs={"ds": "{{ ds }}"},
        requirements=["google-cloud-bigquery"],
        system_site_packages=False,
    )

    # 분기
    def decide_next_task(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='check_bq_partition')
        print(f"task: {result}")
        return result

    branch_after_check = BranchPythonOperator(
        task_id='branch_after_check',
        python_callable=decide_next_task,
        provide_context=True,
    )

    load_parquet_to_bq = BashOperator(
        task_id='load_parquet_partition',
        bash_command="""
        bq load --source_format=PARQUET \
        --hive_partitioning_mode=AUTO \
        --hive_partitioning_source_uri_prefix=gs://jacob-wiki-bucket/bigque/parquet/ko/ \
        load.wiki \
        "gs://jacob-wiki-bucket/bigque/parquet/ko/date={{ ds }}/*.parquet"
        """,
    )

    skip_task = EmptyOperator(task_id='skip_load',trigger_rule='none_failed_min_one_success')
    
    check_gcs >> branch_after_gcs >> [check_bq, skip_task]
    check_bq >> branch_after_check >> [load_parquet_to_bq, skip_task]

