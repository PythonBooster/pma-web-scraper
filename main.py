import requests
from bs4 import BeautifulSoup
import logging
from typing import Optional, Tuple, List
from decouple import Config, RepositoryEnv


config = Config(RepositoryEnv(".env"))


class PhpMyAdminScraper:
    def __init__(
        self, base_url: str, username: str, password: str, db_name: str, table_name: str
    ):
        self.base_url = base_url
        self.login_url = f"{base_url}index.php?route=/"
        self.username = username
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.session = requests.Session()

        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("phpmyadmin_scraper.log", encoding="utf-8"),
            ],
            force=True,  # Перезаписываем существующие настройки логгеров
        )
        self.logger = logging.getLogger(__name__)
        self._setup_session()

    def _setup_session(self) -> None:
        """Настраиваем сессию с заголовками"""
        try:
            self.session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Referer": self.login_url,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Connection": "keep-alive",
                }
            )
            self.logger.info("Сессия успешно настроена")
        except Exception as e:
            self.logger.error(f"Ошибка при настройке сессии: {str(e)}")
            raise

    def _get_login_form_data(self) -> dict:
        """Получаем данные формы для авторизации"""
        try:
            self.logger.info("Получение страницы входа...")
            response = self.session.get(self.login_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            login_form = soup.find("form", {"id": "login_form"})

            if not login_form:
                error_msg = "Форма входа не найдена"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            form_data = {"pma_username": self.username, "pma_password": self.password}

            # Добавляем все скрытые поля из формы
            for hidden in login_form.find_all("input", type="hidden"):
                if "name" in hidden.attrs and "value" in hidden.attrs:
                    form_data[hidden["name"]] = hidden["value"]

            self.logger.debug(f"Данные формы для отправки: {form_data}")
            return form_data

        except requests.RequestException as e:
            self.logger.error(f"Ошибка при получении формы входа: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении формы: {str(e)}")
            raise

    def _check_auth_success(self, response: requests.Response) -> bool:
        """Проверяем успешность авторизации"""
        try:
            if (
                "frame_navigation" not in response.text
                and "pma_navigation_tree" not in response.text
            ):
                self.logger.warning(
                    "Авторизация не удалась. Проверьте логи для деталей."
                )
                return False
            self.logger.info("Авторизация успешна")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при проверке авторизации: {str(e)}")
            return False

    def _parse_table_data(
        self, html: str
    ) -> Tuple[Optional[List[str]], Optional[List[List[str]]]]:
        """Парсим данные таблицы"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            results_table = soup.find("table", {"class": "table_results"})

            if not results_table:
                self.logger.warning("Таблица данных не найдена в HTML")
                return None, None

            # Извлекаем заголовки
            headers = []
            header_cells = results_table.find_all("th", {"class": "column_heading"})
            for th in header_cells:
                header_text = (
                    th.find("a").get_text(strip=True)
                    if th.find("a")
                    else th.get_text(strip=True)
                )
                headers.append(header_text)

            # Извлекаем данные (пропускаем первые 4 столбца с действиями)
            rows = []
            for tr in results_table.find("tbody").find_all("tr"):
                cells = tr.find_all("td")[4:]
                row_data = [td.get_text(strip=True) for td in cells]
                rows.append(row_data)

            self.logger.info(f"Успешно распарсено {len(rows)} строк таблицы")
            return headers, rows

        except Exception as e:
            self.logger.error(f"Ошибка при парсинге таблицы: {str(e)}")
            return None, None

    def _print_results(self, headers: List[str], rows: List[List[str]]) -> None:
        """Выводим результаты в консоль"""
        try:
            print("\nРезультат:")
            print(" | ".join(headers))
            print("-" * (sum(len(h) for h in headers) + 3 * (len(headers) - 1)))
            for row in rows:
                print(" | ".join(row))
            self.logger.info("Результаты успешно выведены в консоль")
        except Exception as e:
            self.logger.error(f"Ошибка при выводе результатов: {str(e)}")
            raise

    def run(self) -> None:
        """Основной метод выполнения скрапинга"""
        try:
            # 1. Авторизация
            form_data = self._get_login_form_data()
            self.logger.info("Отправка данных для авторизации...")

            try:
                response = self.session.post(self.login_url, data=form_data)
                response.raise_for_status()
            except requests.RequestException as e:
                self.logger.error(f"Ошибка при авторизации: {str(e)}")
                return

            if not self._check_auth_success(response):
                return

            # 2. Получаем данные таблицы
            self.logger.info(f"Получение данных таблицы {self.table_name}...")
            table_url = f"{self.base_url}index.php?route=/sql&db={self.db_name}&table={self.table_name}"

            try:
                response = self.session.get(table_url)
                response.raise_for_status()
            except requests.RequestException as e:
                self.logger.error(f"Ошибка при получении таблицы: {str(e)}")
                return

            headers, rows = self._parse_table_data(response.text)
            if headers and rows:
                self._print_results(headers, rows)

        except Exception as e:
            self.logger.critical(
                f"Критическая ошибка в основном потоке: {str(e)}", exc_info=True
            )
        finally:
            try:
                self.session.close()
                self.logger.info("Сессия успешно закрыта")
            except Exception as e:
                self.logger.error(f"Ошибка при закрытии сессии: {str(e)}")


if __name__ == "__main__":
    # Конфигурация

    phpadmin_config = {
        "base_url": config("PMA_BASE_URL"),
        "username": config("PMA_USER_NAME"),
        "password": config("PMA_PASSWORD"),
        "db_name": config("PMA_DB_NAME"),
        "table_name": config("PMA_TABLE_NAME"),
    }

    try:
        scraper = PhpMyAdminScraper(**phpadmin_config)
        scraper.run()
    except Exception as e:
        logging.critical(
            f"Ошибка при создании или запуске скрапера: {str(e)}", exc_info=True
        )
