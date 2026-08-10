# EMR Serverless custom image: the stock EMR Spark runtime + the LATEST
# dbt, so the generic runner (spark_job.py) can invoke dbt in the driver
# exactly as it does locally — one codepath, the platform supplies
# Spark/Iceberg/Glue.
#
# EMR's bundled Python is 3.9 (EOL), which pip would silently resolve to
# an OLD dbt-core. The supported fix (aws-samples custom_python_version):
# install a newer Python and point PYSPARK_PYTHON at it — EMR's pyspark
# lands on PYTHONPATH at runtime, so any driver Python can import it.
#
# NO pip pyspark here: EMR's spark-submit provides its own; a pip install
# would shadow it with the wrong build. (dbt-spark's `session` method only
# needs pyspark importable at runtime.)
#
# Built + pushed by .github/workflows/aws-data-validate.yml; the image URI
# reaches the EMR application via CDK context `emrImageUri`.
FROM public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest
USER root
RUN dnf install -y python3.11 python3.11-pip && \
    python3.11 -m pip install --no-cache-dir \
      "dbt-core>=1.12,<2" "dbt-spark>=1.11,<2" "boto3>=1.34,<2"
ENV PYSPARK_PYTHON=/usr/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11
USER hadoop:hadoop
