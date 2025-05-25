import os
import sys
import random
import threading
import time
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import asyncio
from typing import List, Optional

# Добавляем корневую директорию проекта в путь импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from notifiers.logging import NotificationHandler
from selenium.webdriver.common.by import By
from seleniumbase import SB
from loguru import logger
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from custom_exception import StopEventException
from db_service import SQLiteDBHandler
from locator import LocatorAvito
from xlsx_service import XLSXHandler
from dotenv import load_dotenv
from utils.database import get_async_session
from utils.models import Client

load_dotenv()

async def get_commercial_addresses(report_id: int, session: AsyncSession) -> List[dict]:
    """Получение адресов коммерческих клиентов из базы данных"""
    query = select(Client.id, Client.address).where(
        Client.report_id == report_id,
        #Client.is_commercial == True,
        Client.address.isnot(None)
    )
    result = await session.execute(query)
    return [{"id": row[0], "address": row[1]} for row in result if row[1]]

async def update_client_avito_link(client_id: int, avito_link: str, session: AsyncSession):
    """Обновление ссылки на Авито для клиента"""
    query = update(Client).where(Client.id == client_id).values(
        frod_avito=avito_link,
        frod_procentage=Client.frod_procentage + 30,
        frod_state="Требует внимания"
    )
    await session.execute(query)
    await session.commit()

class AvitoParse:
    """
    Парсинг товаров на avito.ru
    """
    def __init__(self,
                 url: list,
                 keysword_list: list,
                 keysword_black_list: list,
                 count: int = 5,
                 max_price: int = 0,
                 min_price: int = 0,
                 geo: str = None,
                 debug_mode: int = 0,
                 need_more_info: int = 1,
                 proxy: str = None,
                 proxy_change_url: str = "https://changeip.mobileproxy.space/?proxy_key=0a74edb01bb5fb1dd3b845dad96f26d5",
                 stop_event=None,
                 max_views: int = None,
                 fast_speed: int = 0,
                 report_id: Optional[int] = None  # ID отчета
                 ):
        self.url_list = url
        self.url = None
        self.keys_word = keysword_list or None
        self.keys_black_word = keysword_black_list or None
        self.count = count
        self.data = []
        self.title_file = self.__get_file_title()
        self.max_price = int(max_price)
        self.min_price = int(min_price)
        self.max_views = max_views if max_views and max_views != 0 else None
        self.geo = geo
        self.debug_mode = debug_mode
        self.need_more_info = need_more_info
        self.proxy = proxy
        self.proxy_change_url = proxy_change_url
        self.stop_event = stop_event or threading.Event()
        self.db_handler = SQLiteDBHandler()
        self.xlsx_handler = XLSXHandler(self.title_file)
        self.fast_speed = fast_speed
        self.report_id = report_id
        self.addresses = []
        self.current_address_index = 0
        self.current_client_id = None
        self.last_ip_change = 0  # Время последней смены IP
        self.ip_change_interval = 300  # Интервал смены IP в секундах (5 минут)

    async def load_addresses_from_db(self):
        """Загрузка адресов из базы данных"""
        if not self.report_id:
            return

        async for session in get_async_session():
            try:
                self.addresses = await get_commercial_addresses(self.report_id, session)
                logger.info(f"Загружено {len(self.addresses)} адресов из базы данных")
            except Exception as e:
                logger.error(f"Ошибка при загрузке адресов из БД: {e}")
            break

    @property
    def use_proxy(self) -> bool:
        return all([self.proxy, self.proxy_change_url])

    def ip_block(self) -> None:
        """Обработка блокировки IP"""
        if self.use_proxy:
            logger.info("Обнаружена блокировка IP")
            self.change_ip()
        else:
            logger.info("Блок IP. Прокси нет, поэтому делаю паузу")
            time.sleep(random.randint(300, 350))

    def __get_url(self):
        """Модифицированный метод для работы с текущим адресом"""
        if hasattr(self, 'current_address'):
            # Добавляем параметры адреса к URL
            parsed_url = urlparse(self.url)
            query_params = parse_qs(parsed_url.query)
            
            # Добавляем параметры адреса
            query_params['address'] = [self.current_address]
            query_params['radius'] = ['0']  # Точное совпадение
            
            # Формируем новый URL
            new_query = urlencode(query_params, doseq=True)
            self.url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))
        
        logger.info(f"Открываю страницу: {self.url}")
        self.driver.get(self.url)

        if "Доступ ограничен" in self.driver.get_title():
            self.ip_block()
            return self.__get_url()

    def __paginator(self):
        """Кнопка далее"""
        logger.info('Страница загружена. Просматриваю объявления')
        for i in range(self.count):
            if self.stop_event.is_set():
                break
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass
            time.sleep(1)
            self.__parse_page()
            time.sleep(random.randint(2, 4))
            self.open_next_btn()
        return

    def open_next_btn(self):
        self.url = self.get_next_page_url(url=self.url)
        logger.info("Следующая страница")
        self.driver.get(self.url)

    @staticmethod
    def get_next_page_url(url: str):
        """Получает следующую страницу"""
        try:
            url_parts = urlparse(url)
            query_params = parse_qs(url_parts.query)
            current_page = int(query_params.get('p', [1])[0])
            query_params['p'] = current_page + 1

            new_query = urlencode(query_params, doseq=True)
            next_url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query,
                                   url_parts.fragment))
            return next_url
        except Exception as err:
            logger.error(f"Не смог сформировать ссылку на следующую страницу для {url}. Ошибка: {err}")

    def remove_other_cities(self):
        """Удаляет предложения из других городов"""
        try:
            target_div = self.driver.find_elements(LocatorAvito.OTHER_GEO[1], by="css selector")
            if target_div:
                target_div = target_div[0]
            else:
                return

            parent_element = target_div.find_element(By.XPATH, './..')
            time.sleep(2)
            self.driver.execute_script("arguments[0].remove();", parent_element)
            logger.info("Лишние города удалены")

        except Exception as e:
            return

    def __parse_page(self):
        """Парсит открытую страницу"""
        self.check_stop_event()
        all_titles = self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector")
        if all_titles:
            self.remove_other_cities()
            all_titles = self.driver.find_elements(LocatorAvito.TITLES[1], by="css selector")
            logger.info(f"Вижу что-то похожее на объявления")
        titles = [title for title in all_titles if "avitoSales" not in title.get_attribute("class")]
        data_from_general_page = []
        for title in titles:
            """Сбор информации с основной страницы"""
            try:
                name = title.find_element(*LocatorAvito.NAME).text
            except Exception:  # иногда это не объявление
                continue

            if title.find_elements(*LocatorAvito.DESCRIPTIONS):
                try:
                    description = title.find_element(*LocatorAvito.DESCRIPTIONS).text
                except Exception as err:
                    logger.debug(f"Ошибка при получении описания: {err}")
                    description = ''
            else:
                description = ''

            url = title.find_element(*LocatorAvito.URL).get_attribute("href")
            price = title.find_attribute("content")
            ads_id = title.get_attribute("data-item-id")

            if url and not ads_id:
                try:
                    regex = r"_\d+$"
                    ids = re.findall(pattern=regex, string=url)
                    if ids:
                        ads_id = url[-1][:-1]
                    continue
                except Exception:
                    continue

            if not ads_id: continue

            if self.is_viewed(ads_id, price):
                logger.debug("Пропускаю объявление. Уже видел его")
                continue
            data = {
                'name': name,
                'description': description,
                'url': url,
                'price': price,
                'id': ads_id
            }
            all_content = description.lower() + name.lower()
            if self.min_price <= int(price) <= self.max_price:
                if self.keys_word and self.keys_black_word:
                    if any([item.lower() in all_content for item in self.keys_word])\
                            and not any([item.lower() in all_content for item in self.keys_black_word]):
                        data_from_general_page.append(data)
                elif self.keys_black_word:
                    if not any([item.lower() in all_content for item in self.keys_black_word]):
                        data_from_general_page.append(data)
                elif self.keys_word:
                    if any([item.lower() in all_content for item in self.keys_word]):
                        data_from_general_page.append(data)
                else:
                    data_from_general_page.append(data)
        if data_from_general_page:
            self.__parse_other_data(item_info_list=data_from_general_page)

    def __parse_other_data(self, item_info_list: list):
        """Собирает доп. информацию для каждого объявления"""
        for item_info in item_info_list:
            try:
                if self.stop_event.is_set():
                    logger.info("Процесс будет остановлен")
                    break
                if self.need_more_info:
                    item_info = self.__parse_full_page(item_info)

                if self.geo and item_info.get("geo"):  # проверка гео
                    if not self.geo.lower() in str(item_info.get("geo")).lower():
                        continue

                if self.max_views and self.max_views != "0":
                    if int(self.max_views) <= int(item_info.get("views", 0)):
                        logger.info("Количество просмотров больше заданного. Пропускаю объявление")
                        continue

                self.__pretty_log(data=item_info)
                self.__save_data(data=item_info)
            except Exception as err:
                logger.debug(err)

    def __pretty_log(self, data):
        """Красивый вывод для Telegram"""
        price = data.get("price", "-")
        name = data.get("name", "-")
        id_ = data.get("id", "-")
        seller_name = data.get("seller_name")
        full_url = data.get("url")
        short_url = f"https://avito.ru/{id_}"
        # Формируем сообщение для тг
        message = (
                f"*{price}*\n[{name}]({full_url})\n{short_url}\n"
                + (f"Продавец: {seller_name}\n" if seller_name else "")
        )
        try:
            logger.success(message)
        except Exception as err:
            # на случай превышения лимитов
            logger.debug(err)
            time.sleep(61)
            self.__pretty_log(data=data)

    def __parse_full_page(self, data: dict) -> dict:
        """Модифицированный метод для проверки точного адреса и сохранения ссылки"""
        self.driver.get(data.get("url"))
        if "Доступ ограничен" in self.driver.get_title():
            logger.info("Доступ ограничен: проблема с IP")
            self.ip_block()
            return self.__parse_full_page(data=data)

        try:
            self.driver.wait_for_element(LocatorAvito.TOTAL_VIEWS[1], by="css selector", timeout=10)
        except Exception:
            if "Доступ ограничен" in self.driver.get_title():
                logger.info("Доступ ограничен: проблема с IP")
                self.ip_block()
                return self.__parse_full_page(data=data)
            logger.debug("Не дождался загрузки страницы")
            return data

        # Проверяем точный адрес
        if self.addresses and self.driver.find_elements(LocatorAvito.GEO[1], by="css selector"):
            geo = self.driver.find_element(LocatorAvito.GEO[1], by="css selector").text
            data["geo"] = geo.lower()
            
            # Если адрес совпадает, сохраняем ссылку в БД
            if self.current_address.lower() in geo.lower():
                asyncio.run(self.save_avito_link(data.get("url")))
                return data
            return None

        # Остальной код метода остается без изменений
        if self.driver.find_elements(LocatorAvito.TOTAL_VIEWS[1], by="css selector"):
            total_views = self.driver.find_element(LocatorAvito.TOTAL_VIEWS[1]).text.split()[0]
            data["views"] = total_views

        if self.driver.find_elements(LocatorAvito.DATE_PUBLIC[1], by="css selector"):
            date_public = self.driver.find_element(LocatorAvito.DATE_PUBLIC[1], by="css selector").text
            if "· " in date_public:
                date_public = date_public.replace("· ", '')
            data["date_public"] = date_public

        if self.driver.find_elements(LocatorAvito.SELLER_NAME[1], by="css selector"):
            seller_name = self.driver.find_element(LocatorAvito.SELLER_NAME[1], by="css selector").text
            data["seller_name"] = seller_name

        return data

    def is_viewed(self, ads_id: int, price: int) -> bool:
        """Проверяет, смотрели мы это или нет"""
        return self.db_handler.record_exists(ads_id, price)

    def __save_data(self, data: dict) -> None:
        """Сохраняет результат в файл keyword*.xlsx"""
        self.xlsx_handler.append_data(data=data)

        """сохраняет просмотренные объявления"""
        self.db_handler.add_record(record_id=int(data.get("id")), price=int(data.get("price")))

    def __get_file_title(self) -> str:
        """Определяет название файла с учетом текущего адреса"""
        if self.addresses:
            current_address = self.addresses[self.current_address_index]["address"]
            # Создаем безопасное имя файла из адреса
            safe_address = re.sub(r'[^\w\s-]', '', current_address)
            safe_address = re.sub(r'[-\s]+', '_', safe_address).strip('-_')
            return f"result/report_{self.report_id}_address_{safe_address}.xlsx"
        elif self.keys_word not in ['', None]:
            title_file = "-".join(list(map(str.lower, self.keys_word)))
        else:
            title_file = 'all'
        return f"result/{title_file}.xlsx"

    def parse(self):
        """Модифицированный метод для обработки массива адресов"""
        if not self.addresses:
            # Стандартный режим работы без адресов
            self._parse_single_url()
            return

        # Режим работы с массивом адресов
        for address_data in self.addresses:
            if self.stop_event and self.stop_event.is_set():
                logger.info("Процесс будет остановлен")
                return

            self.current_client_id = address_data["id"]
            current_address = address_data["address"]
            logger.info(f"Обработка адреса: {current_address} (ID клиента: {self.current_client_id})")
            self.current_address = current_address
            self.xlsx_handler = XLSXHandler(self.__get_file_title())
            
            try:
                self._parse_single_url()
            except Exception as err:
                logger.error(f"Ошибка при обработке адреса {current_address}: {err}")
                continue

            # Пауза между адресами
            time.sleep(random.randint(5, 10))

    def _parse_single_url(self):
        """Вспомогательный метод для парсинга одного URL"""
        for _url in self.url_list:
            self.url = _url
            if self.stop_event and self.stop_event.is_set():
                logger.info("Процесс будет остановлен")
                return

            # Настройка SOCKS5 прокси
            proxy_settings = {}
            if self.proxy:
                proxy_parts = self.proxy.split('@')
                if len(proxy_parts) == 2:
                    auth, host_port = proxy_parts
                    username, password = auth.split(':')
                    host, port = host_port.split(':')
                    
                    proxy_settings = {
                        'proxyType': 'MANUAL',
                        'socksProxy': f'{host}:{port}',
                        'socksVersion': 5,
                        'socksUsername': username,
                        'socksPassword': password
                    }

            with SB(uc=True,
                    headed=True if self.debug_mode else False,
                    headless2=True if not self.debug_mode else False,
                    page_load_strategy="eager",
                    block_images=True,
                    agent=random.choice(open("user_agent_pc.txt").readlines()),
                    proxy_settings=proxy_settings,  # Используем настройки SOCKS5
                    sjw=True if self.fast_speed else False,
                    ) as self.driver:
                try:
                    self.__get_url()
                    self.__paginator()
                except StopEventException:
                    logger.info("Парсинг завершен")
                    return
                except Exception as err:
                    logger.debug(f"Ошибка: {err}")

    def check_stop_event(self):
        if self.stop_event.is_set():
            logger.info("Процесс будет остановлен")
            raise StopEventException()

    def change_ip(self) -> bool:
        """Смена IP через mobileproxy.space"""
        current_time = time.time()
        
        # Проверяем, прошло ли достаточно времени с последней смены IP
        if current_time - self.last_ip_change < self.ip_change_interval:
            logger.info(f"Ждем {self.ip_change_interval - (current_time - self.last_ip_change):.0f} секунд перед следующей сменой IP")
            time.sleep(self.ip_change_interval - (current_time - self.last_ip_change))
        
        logger.info("Меняю IP")
        try:
            response = requests.get(
                self.proxy_change_url,
                timeout=30,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('status') == 'success':
                        self.last_ip_change = time.time()
                        logger.info(f"IP успешно изменен. Новый IP: {data.get('ip', 'неизвестен')}")
                        return True
                    else:
                        logger.error(f"Ошибка при смене IP: {data.get('message', 'неизвестная ошибка')}")
                except ValueError:
                    logger.error("Не удалось разобрать ответ сервера")
            else:
                logger.error(f"Ошибка при смене IP. Код ответа: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ошибка при смене IP: {str(e)}")
        
        # Если не удалось сменить IP, ждем и пробуем снова
        logger.info("Повторная попытка смены IP через 10 секунд")
        time.sleep(10)
        return self.change_ip()

    async def save_avito_link(self, avito_link: str):
        """Сохранение ссылки на Авито в базу данных"""
        if not self.current_client_id:
            return

        async for session in get_async_session():
            try:
                await update_client_avito_link(self.current_client_id, avito_link, session)
                logger.info(f"Ссылка на Авито сохранена для клиента {self.current_client_id}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении ссылки на Авито: {e}")
            break


if __name__ == '__main__':
    import configparser
    import json
    import sys

    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'settings.ini')
    config.read(config_path, encoding="utf-8")

    try:
        url = config["Avito"]["URL"].split(",")
    except Exception:
        with open(config_path, encoding="utf-8") as file:
            line_url = file.readlines()[1]
            regex = r"http.+"
            url = re.findall(regex, line_url)

    # Получаем ID отчета из аргументов командной строки
    report_id = None
    if len(sys.argv) > 1:
        try:
            report_id = int(sys.argv[1])
        except ValueError:
            logger.error("ID отчета должен быть числом")
            sys.exit(1)

    num_ads = config["Avito"]["NUM_ADS"]
    max_view = config["Avito"].get("MAX_VIEW")
    freq = config["Avito"]["FREQ"]
    keys = config["Avito"]["KEYS"].split(",")
    keys_black = config["Avito"].get("KEYS_BLACK", "").split(",")
    max_price = config["Avito"].get("MAX_PRICE", "9999999999") or "9999999999"
    min_price = config["Avito"].get("MIN_PRICE", "0") or "0"
    geo = config["Avito"].get("GEO", "") or ""
    proxy = config["Avito"].get("PROXY", "aDyT3A:hYa5AKdAtruM@gproxy.site:10693")
    proxy_change_url = config["Avito"].get("PROXY_CHANGE_IP", "https://changeip.mobileproxy.space/?proxy_key=0a74edb01bb5fb1dd3b845dad96f26d5")
    need_more_info = int(config["Avito"]["NEED_MORE_INFO"])
    fast_speed = int(config["Avito"]["FAST_SPEED"])

    if proxy and "@" not in str(proxy):
        logger.info("Прокси переданы неправильно, нужно соблюдать формат user:pass@ip:port")
        proxy = proxy_change_ip = None
    else:
        logger.info("Используется SOCKS5 прокси с автоматической сменой IP")

    while True:
        try:
            parser = AvitoParse(
                url=url,
                count=int(num_ads),
                keysword_list=keys if keys not in ([''], None) else None,
                keysword_black_list=keys_black if keys_black not in ([''], None) else None,
                max_price=int(max_price),
                min_price=int(min_price),
                geo=geo,
                need_more_info=1 if need_more_info else 0,
                proxy=proxy,
                proxy_change_url=proxy_change_ip,
                max_views=int(max_view) if max_view else None,
                fast_speed=1 if fast_speed else 0,
                report_id=report_id
            )
            
            # Загружаем адреса из БД
            asyncio.run(parser.load_addresses_from_db())
            
            if not parser.addresses:
                logger.error("Не найдены адреса для обработки")
                sys.exit(1)
                
            parser.parse()
            logger.info("Пауза")
            time.sleep(int(freq))
        except Exception as error:
            logger.debug(error)
            logger.debug('Произошла ошибка, но работа будет продолжена через 30 сек. '
                         'Если ошибка повторится несколько раз - перезапустите скрипт.'
                         'Если и это не поможет - значит что-то сломалось')
            time.sleep(30)

