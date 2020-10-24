import asyncio
import datetime
import json
import os
from ast import literal_eval
from collections import Counter
from typing import Dict, Any, List, Optional, Tuple

import aiohttp
import typing
from dateutil import relativedelta
import pandas as pd
from tqdm.asyncio import tqdm


class Loader:
    """Синглтон для красивого отображения процесса загрузки"""
    load: Optional[tqdm] = None

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, desc: str = 'Загрузка данных', total_count: Optional[int] = None) -> type(None):
        """
        Создаёт счётчик загрузки
        :param desc: str (optional) default = 'Загрузка данных' Название полосы загрузки
        :param total_count: (optional) Общее количество итерируемых объектов
        """
        if self.load is None:
            self.reset(desc=desc, total_count=total_count)

    def update(self, count: int) -> None:
        if self.load is None:
            self.reset()
        self.load.update(count)

    def reset(self, desc: str = 'Загрузка данных', total_count: Optional[int] = None) -> type(None):
        """
        Обновляет счётчик загрузки
        :param desc: str (optional) default = 'Загрузка данных' Название полосы загрузки
        :param total_count: (optional) Общее количество итерируемых объектов
        """
        try:
            self.load.close()
        except AttributeError:
            pass
        self.load = tqdm(desc=desc, total=total_count, unit='ШТ', ncols=100)


async def get_response(sem: asyncio.Semaphore, session: aiohttp.ClientSession, url: str,
                       params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Отправка запроса
    :param url: адрес запроса
    :param sem Semaphore Семафор для ограничения количества одновременных запросов
    :param params: dict параметры запроса
    :param session: сессия для запросов
    :return dict возвращает данные ответа, если запрос успешный
    """
    async with sem:
        response = await session.get(url, params=params)
        result: Optional[Dict[str, Any]] = None
        if response.status == 200:
            try:
                result = await response.json()
            except aiohttp.ClientPayloadError:
                await get_response(sem, session, url, params)  # костыль, но работает
            else:
                Loader().update(1)
        elif response.status == 404:
            Loader().update(1)
        else:
            while result is None:
                result = await get_response(sem, session, url, params)  # костыль, но работает
    return result


async def get_items(sem: asyncio.Semaphore, session: aiohttp.ClientSession, params: Dict[str, Any],
                    page: int = 0) -> list:
    """Асинхронное получение данных
    :param sem Semaphore Семафор для ограничения количества одновременных запросов
    :param session сессия для запросов
    :param params dict первоночальные параметры запроса
    :param page (optional) default = 0 Номер начальной страницы
    :return list полученные данные
    """
    items: List[Any] = []
    params.update({'page': page})
    data: Optional[Dict[str, Any]] = await get_response(sem, session, 'https://api.hh.ru/vacancies', params)
    if data:
        items.extend(data['items'])
        if data['pages'] and False:  # Если есть страницы, то пробегает по всем страницам
            while data['pages'] > params['page']:
                items.extend(await get_items(sem, session, params, page=params['page'] + 1))
    return items


async def get_raw_data() -> asyncio.Future:
    """Загрузка сырых данных"""
    now: datetime.datetime = datetime.datetime.now()
    start_date: datetime.datetime = now
    end_date: datetime.datetime = now
    prev_month: datetime.datetime = now - relativedelta.relativedelta(months=1)
    async with aiohttp.ClientSession() as session:
        tasks = []
        sem = asyncio.Semaphore(50)
        while start_date > prev_month:
            start_date -= datetime.timedelta(hours=1)
            params = {
                'specialization': 1,
                'date_from': start_date.isoformat(),
                'date_to': end_date.isoformat(),
                'per_page': 100,
            }
            task = asyncio.ensure_future(get_items(sem, session, params=params))
            tasks.append(task)
            end_date = start_date
        return await asyncio.gather(*tasks)


async def get_detail_raw_data(data: List[int]) -> asyncio.Future:
    """Загрузка подробных сырых данных
    :param: data список id вакансий
    """
    Loader().reset(desc='Загрузка подробных данных', total_count=len(data))
    async with aiohttp.ClientSession() as session:
        tasks = []
        sem = asyncio.Semaphore(50)
        for v_id in data:
            task = asyncio.ensure_future(get_response(sem, session, f'https://api.hh.ru/vacancies/{v_id}'))
            tasks.append(task)
        return await asyncio.gather(*tasks)


def upload_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загрузка данных
    :param file_name (optional) default = None
    :return: DataFrame
    """
    file_name: str = file_name or datetime.datetime.now().isoformat()
    loop: asyncio = asyncio.get_event_loop()
    df: pd.DataFrame = pd.DataFrame([item for items in loop.run_until_complete(get_raw_data()) for item in items])
    df.drop_duplicates(subset=['id'], inplace=True)
    if not os.path.exists('data'):
        os.makedirs('data')
    df.to_csv(os.path.join('data', f'{file_name}.csv'))
    return df


def load_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загружает данные из файла если есть, либо создаёт такой файл
    :param file_name: (optional) имя файла
    :return: DataFrame данные
    """
    file_name: str = file_name or datetime.datetime.now().isoformat()
    if os.path.exists(os.path.join('data', f'{file_name}.csv')):
        df = pd.read_csv(os.path.join('data', f'{file_name}.csv'))
    else:
        df = upload_data(file_name)
    return df


def load_details_raw_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загружает данные из файла если есть, либо создаёт такой файл
    :param file_name: (optional) имя файла
    :return: DataFrame данные
    """
    file_name: str = file_name or datetime.datetime.now().isoformat()
    if os.path.exists(os.path.join('data', f'detail_{file_name}.json')):
        df: pd.DataFrame = pd.read_json(os.path.join('data', f'detail_{file_name}.json'))
    elif os.path.exists(os.path.join('data', f'detail_{file_name}.csv')):
        df: pd.DataFrame = pd.read_csv(os.path.join('data', f'detail_{file_name}.csv'))
        objs: List[str] = ['key_skills', 'schedule', 'experience', 'address', 'department', 'employment', 'salary',
                           'insider_interview', 'area', 'employer', 'type', 'test', 'specializations', 'contacts',
                           'billing_type', 'driver_license_types', 'working_days', 'working_time_intervals',
                           'working_time_modes']
        for obj in objs:
            df[obj] = df[obj].apply(lambda x: literal_eval(x) if not pd.isna(x) else None)
    else:
        df: pd.DataFrame = load_data(file_name)
        loop = asyncio.get_event_loop()
        data = [item for item in loop.run_until_complete(get_detail_raw_data(list(df['id']))) if item]
        json.dump(data, open(os.path.join('data', f'detail_{file_name}.json'), 'w'))
        df = pd.DataFrame(data)
        df.to_csv(os.path.join('data', f'detail_{file_name}.csv'))
    return df


def clear_specializations(data: List[Dict]) -> Tuple[int, List, List, List, List]:
    """
    Обработка поля specializations
    :param data: dict Данные поля specializations
    :return: tuple кортеж из спискох необходимых полей
    """
    specializations_id: List[int] = []
    specializations_name: List[str] = []
    specializations_profarea_id: List[int] = []
    specializations_profarea_name: List[str] = []
    for dt in data:
        specializations_id.append(int(dt['id'].replace('.', '')))
        specializations_name.append(dt['name'])
        specializations_profarea_id.append(int(dt['profarea_id']))
        specializations_profarea_name.append(dt['profarea_name'])
    return len(data), specializations_id, specializations_name, list(set(specializations_profarea_id)),\
           list(set(specializations_profarea_name))


def clear_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Очищаеются и подготавливаются данные
    :param data: DataFrame данные которые необходимо обработать
    :return: DataFrame обработанные данные
    """
    Loader().reset(desc='Очистка данных', total_count=15)
    cleared_df = data[['id', 'key_skills', 'schedule', 'salary', 'name', 'archived',
                       'specializations', 'experience', 'employment', 'area', 'working_days',
                       'working_time_intervals', 'working_time_modes', 'accept_temporary']].copy()
    Loader().update(1)
    # Оставляем только нужные поля
    cleared_df['specializations_count'], cleared_df['specializations_id'], cleared_df['specializations_name'],\
    cleared_df['specializations_profarea_id'], cleared_df['specializations_profarea_name']\
        = zip(*cleared_df['specializations'].apply(clear_specializations))  # Обрабатываем данные поля specializations
    Loader().update(1)
    for field in ['experience', 'schedule', 'employment', 'area']:
        cleared_df[f'{field}_id'], cleared_df[f'{field}_name'] = \
            zip(*cleared_df[field].apply(lambda s: (s.get('id'), s.get('name')) if not pd.isna(s) else (None, None)))
        Loader().update(1)
    for field in ['working_days', 'working_time_intervals', 'working_time_modes']:
        cleared_df[f'{field}_count'], cleared_df[f'{field}_id'], cleared_df[f'{field}_name'] = \
            zip(*cleared_df[field].apply(
                lambda l: (len(l), [i.get('id') for i in l], [i.get('name') for i in l]) if not pd.isna(l) else (
                    None, None, None)))
        Loader().update(1)
    cleared_df['salary_from'], cleared_df['salary_to'], cleared_df['currency'], cleared_df['gross'] = \
        zip(*cleared_df['salary'].apply(
            lambda s: (s.get('from'), s.get('to'), s.get('currency'), s.get('gross')) if not pd.isna(s) else (
                None, None, None, None)))
    Loader().update(1)
    cleared_df['key_skills_count'], cleared_df['key_skills'] = \
        zip(*cleared_df['key_skills'].apply(lambda l: (len(l), [s.get('name') for s in l]) if l else (None, None)))
    Loader().update(1)
    cleared_df = cleared_df[cleared_df['archived'] == False]
    Loader().update(1)
    cleared_df = cleared_df[cleared_df['currency'] == 'RUR']
    Loader().update(1)
    cleared_df = cleared_df.drop(['experience', 'schedule', 'employment', 'area', 'experience', 'working_days',
                                  'working_time_intervals', 'working_time_modes', 'specializations', 'salary',
                                  'archived'], axis=1)
    Loader().update(1)
    return cleared_df


def create_date(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загружает очищенные данные из файла если есть, либо создаёт такой файл
    :param file_name: (optional) имя файла
    :return: DataFrame данные
    """
    file_name: str = file_name or datetime.datetime.now().isoformat()
    if os.path.exists(os.path.join('data', f'finall_{file_name}.json')):
        df: pd.DataFrame = pd.read_json(os.path.join('data', f'finall_{file_name}.json'))
    elif os.path.exists(os.path.join('data', f'finall_{file_name}.csv')):
        df: pd.DataFrame = pd.read_csv(os.path.join('data', f'finall_{file_name}.csv'))
        objs: List[str] = ['specializations', 'specializations_profarea', 'working_days', 'working_time_intervals',
                           'working_time_modes']
        for obj in objs:
            df[f'{obj}_id'] = df[f'{obj}_id'].apply(lambda x: literal_eval(x) if not pd.isna(x) else None)
            df[f'{obj}_name'] = df[f'{obj}_name'].apply(lambda x: literal_eval(x) if not pd.isna(x) else None)
    else:
        df: pd.DataFrame = load_details_raw_data(file_name)
        df = clear_data(df)
        df.to_json(os.path.join('data', f'finall_{file_name}.json'))
        df.to_csv(os.path.join('data', f'finall_{file_name}.csv'))
    return df


def show_skills(df: pd.DataFrame) -> None:
    """
    Вывод графика наиболее востребованных скиллов
    :param df dataframe Данные из которых строится график
    """
    key_skills_all: List[str] = []
    for row in df['key_skills']:
        if row:
            key_skills_all.extend(row)
    counter_skills: typing.Counter = Counter(key_skills_all)
    import matplotlib.pyplot as plt
    import numpy as np
    plt.style.use('seaborn')
    x = [item[0] for item in counter_skills.most_common(10)]
    y2 = [item[1] for item in counter_skills.most_common(10)]
    fig, ax = plt.subplots()

    col = np.random.rand(10, 3)
    ax.bar(x, y2, color=col, label='Количество')
    ax.legend()
    plt.xlabel('Навыки')
    plt.ylabel('Количество упоминаний')
    plt.xticks(rotation=45)  # поворот текста в графике
    fig.set_figheight(8)
    fig.set_figwidth(12)
    plt.show()


if __name__ == '__main__':
    df = create_date('test')
    show_skills(df)
