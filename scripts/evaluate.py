import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
import joblib
import json
import yaml
import os

# оценка качества модели
def evaluate_model():
    # прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
        # загрузите результат прошлого шага: fitted_model.pkl
    data = pd.read_csv('data/initial_data.csv')    
    pipeline = joblib.load('models/fitted_model.pkl')
    # реализуйте основную логику шага с использованием прочтённых гиперпараметров
    cv_strategy = StratifiedKFold(n_splits=params['n_splits'])
    cv_res = cross_validate(
        pipeline,
        data.drop([params['target_col']], axis=1),
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics']
    )
    os.makedirs('cv_results', exist_ok=True)
    for key, value in cv_res.items():
        cv_res[key] = round(value.mean(), 3) 
    # сохраните результата кросс-валидации в cv_res.json
    with open('cv_results/cv_res.json', 'w') as f:
        json.dump(cv_res, f)

if __name__ == '__main__':
    evaluate_model() 