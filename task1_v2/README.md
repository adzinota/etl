[Видео](https://disk.yandex.ru/i/kKOkC8c6IzoK6w)

Задание №1
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. Недавно банку потребовалось построить отчёт по 101 форме. Те данные, что остались в csv-файлах, тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД Oracle / PostgreSQL.

Задача 1.1
Разработать ETL-процесс для загрузки «банковских» данных из csv-файлов в соответствующие таблицы СУБД Oracle или PostgreSQL. Покрыть данный процесс логированием этапов работы и всевозможной дополнительной статистикой (на усмотрение вашей фантазии). В исходных файлах могут быть ошибки в виде некорректных форматах значений. Но глядя на эти значения вам будет понятно, какие значения имеются в виду.

Исходные данные:
* Данные из 6 таблиц в виде excel-файлов:
* md_ledger_account_s – справочник балансовых счётов;
* md_account_d – информация о счетах клиентов;
* ft_balance_f – остатки средств на счетах;
* ft_posting_f – проводки (движения средств) по счетам;
* md_currency_d – справочник валют;
* md_exchange_rate_d – курсы валют.
* Файл «Структура таблиц.docx» – поможет создать таблицы в детальном слое DS.

Требования к реализации задачи:
* В своей БД создать пользователя / схему «DS».
* Создать в DS-схеме таблицы под загрузку данных из csv-файлов.
* Начало и окончание работы процесса загрузки данных должно логироваться в специальную логовую таблицу. Эту таблицу нужно придумать самостоятельно;
* После логирования о начале загрузки добавить таймер (паузу) на 5 секунд, чтобы чётко видеть разницу во времени между началом и окончанием загрузки. Из-за небольшого учебного объёма данных – процесс загрузки быстрый;
* Для хранения логов нужно в БД создать отдельного пользователя / схему «LOGS» и создать в этой схеме таблицу для логов;
* Для корректного обновления данных в таблицах детального слоя DS нужно выбрать правильную Update strategy и использовать следующие первичные ключи для таблиц фактов, измерений и справочников (должно быть однозначное уникальное значение, идентифицирующее каждую запись таблицы):

|Таблица | Первичный ключ |
| ------------- |:------------------:| 
| DS.FT_BALANCE_F | ON_DATE, ACCOUNT_RK |
| DS.FT_POSTING_F | OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK |
| DS.MD_ACCOUNT_D | DATA_ACTUAL_DATE, ACCOUNT_RK |
| DS.MD_CURRENCY_D | CURRENCY_RK, DATA_ACTUAL_DATE |
| DS.MD_EXCHANGE_RATE_D | DATA_ACTUAL_DATE, CURRENCY_RK |
| DS.MD_LEDGER_ACCOUNT_S | LEDGER_ACCOUNT, START_DATE |

Структура таблиц

DS.FT_BALANCE_F

|Файл | База |
| ------------- |:------------------:| 
| on_date | DATE not null |
| account_rk | NUMBER not null |
| currency_rk | NUMBER |
| balance_out | FLOAT |

DS.FT_POSTING_F

|Файл | База |
| ------------- |:------------------:| 
| oper_date | DATE not null |
| credit_account_rk | NUMBER not null |
| debet_account_rk | NUMBER not null |
| credit_amount | FLOAT |
| debet_amount | FLOAT |

DS.MD_ACCOUNT_D

|Файл | База |
| ------------- |:------------------:| 
| data_actual_date | DATE not null |
| data_actual_end_date | DATE not null |
| account_rk | NUMBER not null |
| account_number | VARCHAR2(20 char) not null |
| char_type | VARCHAR2(1 char) not null |
| currency_rk | NUMBER not null |
| currency_code | VARCHAR2(3 char) not null |

DS.MD_CURRENCY_D

|Файл | База |
| ------------- |:------------------:| 
| currency_rk | NUMBER not null |
| data_actual_date | DATE not null |
| data_actual_end_date | DATE |
| currency_code | VARCHAR2(3 char) |
| code_iso_char | VARCHAR2(3 char) |

DS.MD_EXCHANGE_RATE_D

|Файл | База |
| ------------- |:------------------:| 
| data_actual_date | DATE not null |
| data_actual_end_date | DATE |
| currency_rk | NUMBER not null |
| reduced_cource | FLOAT |
| code_iso_num | VARCHAR2(3 char) |

DS.MD_LEDGER_ACCOUNT_S

|Файл | База |
| ------------- |:------------------:| 
| chapter | CHAR(1 char) |
| chapter_name | VARCHAR2(16 char) |
| section_number | INTEGER |
| section_name | VARCHAR2(22 char) |
| subsection_name | VARCHAR2(21 char) |
| ledger1_account | INTEGER |
| ledger1_account_name | VARCHAR2(47 char) |
| ledger_account | INTEGER not null |
| ledger_account_name | VARCHAR2(153 char) |
| characteristic | CHAR(1 char) |
| is_resident | INTEGER, |
| is_reserve | INTEGER, |
| is_reserved | INTEGER, |
| is_loan | INTEGER, |
| is_reserved_assets | INTEGER, |
| is_overdue | INTEGER |
| is_interest | INTEGER |
| pair_account | VARCHAR2(5 char) |
| start_date | DATE not null |
| end_date | DATE |
| is_rub_only | INTEGER |
| min_term | VARCHAR2(1 char) |
| min_term_measure | VARCHAR2(1 char) |
| max_term | VARCHAR2(1 char) |
| max_term_measure | VARCHAR2(1 char) |
| ledger_acc_full_name_translit | VARCHAR2(1 char) |
| is_revaluation | VARCHAR2(1 char) |
| is_correct | VARCHAR2(1 char) |