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

# 3. Начальное заполение данных

```shell
python src/data/initiate.py
```
