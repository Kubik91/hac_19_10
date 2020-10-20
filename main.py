import asyncio
import datetime
import os
from typing import Dict, Any, List, Optional

import aiohttp
from dateutil import relativedelta
from pipenv.vendor import requests
import pandas as pd


async def get_response(session: aiohttp.ClientSession, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Отправка запроса
    :arg params dict параметры запроса
    :arg session сессия для запросов
    :return dict возвращает данные ответа, если запрос успешный
    """
    response = await session.get('https://api.hh.ru/vacancies', params=params)
    result = None
    if response.status == 200:
        result = await response.json()
    return result


async def get_items(sem: asyncio.Semaphore, session: aiohttp.ClientSession, params: Dict[str, Any], page: int = 0) -> list:
    """Асинхронное получение данных
    :arg sem Semaphore Семафор для ограничения количества одновременных запросов
    :arg session сессия для запросов
    :arg params dict первоночальные параметры запроса
    :arg page (optional) default = 0 Номер начальной страницы
    :return list полученные данные
    """
    items: List[Any] = []
    params.update({'page': page})
    async with sem:
        data = await get_response(session, params)
        if data:
            items.extend(data['items'])
            if data['pages'] and False:  # Если есть страницы, то пробегает по всем страницам
                while data['pages'] > params['page']:
                    items.extend(await get_items(sem, session, params, page=params['page'] + 1))
    return items


async def get_raw_data():
    """Загрузка сырых данных
    :rtype: object
    """
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


def upload_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загрузка данных
    :arg file_name (optional) default = None
    :return: DataFrame
    """
    loop = asyncio.get_event_loop()
    # print(type([item for items in loop.run_until_complete(get_raw_data()) for item in items]))
    df = pd.DataFrame([item for items in loop.run_until_complete(get_raw_data()) for item in items])
    df.drop_duplicates(subset=['id'], inplace=True)
    if not os.path.exists('data'):
        os.makedirs('data')
    print(len(df))
    df.to_csv(os.path.join('data', file_name or f'{datetime.datetime.now().isoformat()}.csv'))
    return df


def load_data(file_name: Optional[str] = None) -> pd.DataFrame:
    """Загружает данные из файла если есть, либо создаёт такой файл
    :arg file_name: (optional) имя файла
    :return: DataFrame данные
    """
    file_name = file_name or datetime.datetime.now().isoformat()
    if os.path.exists(os.path.join('data', file_name)):
        df = pd.read_csv(os.path.join('data', file_name))
    else:
        df = upload_data(file_name or f'{datetime.datetime.now().isoformat()}.csv')
    return df


if __name__ == '__main__':
    upload_data()
