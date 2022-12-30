card-fraud-detection
==============================

Проект обучения модели, определяющей мошеннические транзакции пластиковых карт.

# 1. Структура проекта
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------
# 2. Цели и метрики

Целью проекта является модель, способная классифицировать мошеннические транзакции. 
Данная задача относится к задачам бинарной классификации.
В качестве метрик качества обечения используются **recall**, **F1-score** и **AUC ROC**. 
Указанные метрики выбраны, так как наиболее негативным сценарием применения модели является ошибка второго рода (пропуск цели) или ошибка false negative: транзакция классифицирована как валидная при условии, что она является мошеннической. 

Датасет используемый для обучения модели является сильно несбаллансированным. 
Т.к. рассматриваемая задача относится к задаче бинарной классификации, то cost matrix для нее будет следующей: 

$$ c_{00} = c_{11} = 0, c_{01} = 1, c_{10} = IR, $$

где $IR$ - imbalance rate, отношение количества мошеннических транзакций к колличеству легальных транзакций в датасете.

--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

# 3. Работат с датасетом

Создать датасет
```shell
python src/data/manage.py create --n-customers=150000 --n-terminals=10000 --start-date=2022-01-01 --nb-days=25 --radius=10
```

Дополнить датасет
```shell
python src/data/manage.py update --start-date=2022-02-01 --nb-days=25 --radius=5
```

Удалить датасет
```shell
python src/data/manage.py delete
```

```shell
python3 -m virtualenv --copies venv
```

```shell
venv-pack -o venv.tar.gz
```

```shell
PYTHON_VENV='./venv/bin/python'
export PYSPARK_PYTHON=${PYTHON_VENV}
spark-submit --master yarn --deploy-mode cluster \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYTHON_VENV} \
--archives ./venv.tar.gz#venv \
src/data/yandex-example.py s3a://yl-otus/yandex-data.txt s3a://yl-otus/output
```

```shell
PYTHON_VENV='./venv/bin/python'
export PYSPARK_PYTHON=${PYTHON_VENV}
spark-submit --master yarn --deploy-mode cluster \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYTHON_VENV} \
--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${PYTHON_VENV} \
--archives s3a://yl-otus/venv.tar.gz#venv \
s3a://yl-otus/yandex-example.py s3a://yl-otus/yandex-data.txt s3a://yl-otus/output
```

```shell
PYTHON_VENV='./venv/bin/python'
export PYSPARK_PYTHON=${PYTHON_VENV}
spark-submit --master yarn --deploy-mode cluster \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYTHON_VENV}\
--num-executors 5 --executor-cores 5 \
--driver-memory 8g --executor-memory 16g \
--archives ./venv.tar.gz#venv \
src/data/update-dataset.py
```

```shell
yc dataproc job create-pyspark --cluster-id c9q1ee8pnj1r46ogk49s \
--name yc-manual-launch \
--main-python-file-uri s3a://yl-otus/yandex-example.py \
--args s3a://yl-otus/yandex-data.txt --args=s3a://yl-otus/output \
--archive-uris s3a://yl-otus/venv.tar.gz#venv \
--properties spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/bin/python \
--properties spark.executorEnv.PYSPARK_PYTHON=./venv/bin/python \
--properties spark.submit.deployMode=cluster \
--properties spark-env:PYSPARK_PYTHON=./venv/bin/python \
--properties hadoop-env:PYSPARK_PYTHON=./venv/bin/python
```

```shell
yc dataproc job create-pyspark --cluster-id=c9qe7r6747r6hidd2p05 \
--name=yc-manual-launch \
--main-python-file-uri ./card-fraud-detection.tar.gz/card-fraud-detection/src/data/yandex-example.py \
--args s3a://yl-otus/yandex-data.txt --args s3a://yl-otus/output \
--archive-uris s3a://yl-otus/venv.tar.gz#venv \
--archive-uris s3a://yl-otus/card-fraud-detection.tar.gz \
--properties spark.yarn.appMasterEnv.PYSPARK_PYTHON=venv/bin/python \
--properties spark.submit.deployMode=client
```

```shell
spark-submit --master yarn --deploy-mode cluster \
--files s3a://yl-otus/requirements.txt \
--conf spark.pyspark.virtualenv.enabled=true  \
--conf spark.pyspark.virtualenv.type=native \
--conf spark.pyspark.virtualenv.requirements=./requirements.txt \
--conf spark.pyspark.virtualenv.bin.path=/opt/conda/bin/virtualenv \
--conf spark.pyspark.python=/usr/bin/python3 \
s3a://yl-otus/cloudera.py
```

```shell
yc dataproc job log --cluster-id=c9q1ee8pnj1r46ogk49s c9qslupu9ch0v2u33lrb
```

worked

```shell
PYTHON_VENV='./venv/bin/python'
spark-submit --master yarn --deploy-mode cluster \
--conf spark.executorEnv.PYSPARK_PYTHON=${PYTHON_VENV} \
--archives s3a://yl-otus/venv.tar.gz#venv s3a://yl-otus/cloudera.py
```

```shell
PYTHON_VENV='./venv/bin/python'
spark-submit --master yarn --deploy-mode client \
--conf spark.executorEnv.PYSPARK_PYTHON=${PYTHON_VENV} \
--archives s3a://yl-otus/venv.tar.gz#venv s3a://yl-otus/cloudera.py
```

```shell
PYTHON_VENV='./venv/bin/python'
yc dataproc job create-pyspark --cluster-id c9qe7r6747r6hidd2p05 \
--name yc-manual-launch \
--main-python-file-uri s3a://yl-otus/cloudera.py \
--archive-uris s3a://yl-otus/venv.tar.gz#venv \
--properties spark.executorEnv.PYSPARK_PYTHON=${PYTHON_VENV} \
--properties spark.submit.deployMode=cluster
```