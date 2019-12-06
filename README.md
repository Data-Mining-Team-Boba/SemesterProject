# SemesterProject

## Virtual environment
* Creating: `virtualenv <path to env>/pyspark-env`
* Using: `source <path to env>/pyspark-env/bin/activate`
* Install packages: `pip3 install --requirement requirements.txt`

## Copy to Compute Engine:
```
gcloud compute scp --project="chess-attack-defend-power" --zone="us-central1-a" parser.py mongo-and-api:~/
```

## Spark installation (local)
