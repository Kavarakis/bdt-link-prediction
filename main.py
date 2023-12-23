import subprocess
import sys


def run_spark_job(job_script):
    # Construct the spark-submit command
    spark_submit_cmd = ["spark-submit", job_script]

    # Run the spark-submit command
    result = subprocess.run(
        spark_submit_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Print outputs for debugging
    print(result.stdout)
    print(result.stderr, file=sys.stderr)

    # Check for errors
    if result.returncode != 0:
        raise Exception(f"Error running job {job_script}")


def main():
    # List of your Spark job scripts
    job_scripts = [
        "/path/to/jobs/job1.py",
        "/path/to/jobs/job2.py"
        # Add other jobs as needed
    ]

    # Iterate and submit each job
    for job_script in job_scripts:
        print(f"Running job: {job_script}")
        run_spark_job(job_script)


if __name__ == "__main__":
    main()
