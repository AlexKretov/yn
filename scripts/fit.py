import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder
from sklearn.preprocessing import OneHotEncoder
from catboost import CatBoostRegressor
from sklearn.impute import SimpleImputer
import yaml
import os
import joblib

# обучение модели
def fit_model():
    # Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    print('Params loaded sucessfully')
    # загрузите результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    print('Data loaded sucessfully')
    # Построение пайплайна
    cat_features = data.select_dtypes(include='object')

    fit_params = {
        'learning_rate': params['learning_rate'],
        'min_child_samples': params['min_child_samples'],
        'depth': params['depth'],
        'loss_function': params['loss_function'],
        'random_state': params['random_state'],
    }
    model = CatBoostRegressor(**fit_params, cat_features=cat_features.columns.tolist())

    model.fit(data.drop([params['target_col']], axis=1), data[params['target_col']]) 
    print('Model  fitted sucessfully')
    os.makedirs('models', exist_ok=True)
    # сохраните обученную модель в models/fitted_model.pkl
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(model, fd)
    print('Model saved sucessfully!')
if __name__ == '__main__':
    fit_model()


    