import asyncio
import datetime
import json
import os
from ast import literal_eval
from typing import Dict, Any, List, Optional

import aiohttp
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
                Loader().load.update(1)
        elif response.status == 404:
            pass
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
        df = upload_data(f'{file_name}.csv')
    return df


def load_details_raw_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загружает данные из файла если есть, либо создаёт такой файл
    :param file_name: (optional) имя файла
    :return: DataFrame данные
    """
    file_name: str = file_name or datetime.datetime.now().isoformat()
    if os.path.exists(os.path.join('data', f'detail_{file_name}.csv')):
        df: pd.DataFrame = pd.read_csv(os.path.join('data', f'detail_{file_name}.csv'))
    else:
        df: pd.DataFrame = load_data(file_name)
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(get_detail_raw_data(list(df['id'])))
        json.dump(data, open(os.path.join('data', f'detail_{file_name}.json'), 'w'))
        df = pd.DataFrame([item for item in data if item])
        df.to_csv(os.path.join('data', f'detail_{file_name}.csv'))
    return df


if __name__ == '__main__':
    df = load_details_raw_data('test')
    print(df.columns)
    df['specializations'] = df['specializations'].apply(literal_eval)
    # df: pd.DataFrame = pd.read_csv(os.path.join('data', f'detail_5_test.csv'))
    # print(df.columns)
    # df[['id', 'key_skills', 'schedule', 'schedule', 'employment', 'salary', 'name', 'area', 'specializations',
    #     'schedule', 'experience', 'employment', 'area', 'working_days', 'working_time_intervals', 'working_time_modes',
    #     'accept_temporary']].to_csv(os.path.join('data', f'pre_detail_5_test.csv'), index=False)
