import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor 
from airflow.providers.http.hooks.http import HttpHook

class MPTSHooks(HttpHook):
    """Хук для работы с API МПТС."""
    def __init__(self, mpts_conn_id: str = 'mpts_default'): # принимает адрес сервера и ключ доступа
        super().__init__(method='POST', http_conn_id=mpts_conn_id)
        
        # кэширование параметров подключения при инициализации (хранение данных в оперативной памяти воркера)
        self._api_key = None
        self._cached_data = {}

    # безопасное извлечение ключа    
    def _get_api_key(self):
        if self._api_key is None:
            conn = self.get_connection(self.http_conn_id)
            self._api_key = conn.password
        return self._api_key
    
    # формирование и отправка запроса
    def get_data(self, function, params=None):
        payload = {
            "key": self._get_api_key(),
            "function": function,
            "params": params
        }
        
        # заголовки и отправка
        headers = {
            'user-agent': "airflow-mpts-client",
            'content-type': "application/json; charset=unicode"
        }

        response = self.run( # передача сформированного json и заголовки
            endpoint="api/v1/api.php",
            data=json.dumps(payload),
            headers=headers,
            extra_options={"verify": False} # игнорировать проверку SSL-сертификатов корпоративного сервера
        )
        
        # валидация ответа сервера
        res_json = response.json()

        if res_json.get('success') is True or res_json.get('succes') is True or 'data' in res_json:
            return res_json.get('data')
        
        raise ValueError(f"Бизнес-ошибка API в функции {function}: {res_json}")

    # кэширование результатов функции
    def _get_cached_list(self, function_name):
        data = self._cached_data.setdefault(function_name, self.get_data(function=function_name))
        return list(data)
    
    # запрашивается у API справочники; используется для замены ID на название 
    def getSwitchingTypeList(self):
        """Получение справочника списка типа отключения."""
        return self._get_cached_list("getSwitchingTypeList")
    
    def getCompanyList(self):
        """Получение справочника списка компаний."""
        return self._get_cached_list("getCompanyList")   
    
    def getSwitchingStatusList(self):
        """Получение справочника статусов отключений."""
        return self._get_cached_list("getSwitchingStatusList")    
    
    def getDefectStatusList(self):
        """Получение справочника статусов дефектов."""
        return self._get_cached_list("getDefectStatusList")    
    
    def getHeatingSupplyImpactList(self):
        """Получение справочника 'Влияния дефекта на подачу теплоснабжения/ГВС'."""
        return self._get_cached_list("getHeatingSupplyImpactList")     
    
    def getViolationTypeList(self):
        """Получение справочника 'Видов нарушений'."""
        return self._get_cached_list("getViolationTypeList")
    
    def getSwitchingCauseList(self):
        """Получение справочника причин отключений."""
        return self._get_cached_list("getSwitchingCauseList")
    
    # возвращает время ввиде строки  
    @staticmethod
    def datetimeTZOffset(_dt, companyID, companyTZOffset):
        if not _dt:
            return None
        if isinstance(_dt, str):
            if 'T' in _dt:
                _dt = datetime.strptime(_dt, "%Y-%m-%dT%H:%M:%SZ")
            else:
                _dt = datetime.strptime(_dt, "%Y-%m-%d %H:%M:%S.%f")
        elif not isinstance(_dt, datetime):
            raise TypeError(f"Неподдерживаемый тип для даты: {type(_dt)}")
        
        minutes_offset = companyTZOffset.get(companyID, 0)
        return _dt + timedelta(minutes=minutes_offset) # прибавляется смещение конкретной компании
        
    # используется для инкрементальной загрузки
    def getSwitchingChangesForPeriod(self, companyID=None, periodStartDate=None, periodEndDate=None):
        """Получение факта создания/изменения/удаления отключений за указанный период."""

        # быстрая валидация и формирование дат
        def parse_date(dt, datetime):
            if hasattr(dt, 'strftime'):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(dt, str):
                return dt
            raise ValueError("Параметр {name} не определен или имеет неверный тип")
        
        start_str = parse_date(periodStartDate, "periodStartDate")
        end_str = parse_date(periodEndDate, "periodEndDate")
        
        # формирование параметров запроса
        params = {"periodStartDate": start_str, "periodEndDate": end_str}
        if companyID is not None:
            params["companyID"] = companyID
        
        # запрос данных
        data = self.get_data(function="getSwitchingChangesForPeriod", params=params)
        if not data:
            return []
        
        # использование кэшированного списка компаний
        companies = self.getCompanyList() or []
        company_tz_offset = {row.get('companyID'): row.get('tzOffset') for row in companies if 'companyID' in row}
        
        # локальная копия метода
        tz_offset_func = self.datetimeTZOffset
        for row in data:
            row_company_id = row.get('companyID') or companyID

            row['changeDate'] = tz_offset_func(
                row.get('changeDate'),
                row_company_id,
                company_tz_offset
            )
            
        return data
    
    def getSwitchingByNumber(self, switchingNumber=None, switchingStatusList=None, switchingTypeList=None, switchingCauseList=None, heatingSupplyImpactList=None, companyTZOffset=None):
        """Получение подробной информации по отключению с заданным номером и денормализация."""
        if not switchingNumber:
            raise ValueError("Параметр switchingNumber обязателен для заполнения")
        
        params = {"switchingNumber": switchingNumber} # получение номера конкретного отключения
        # подтягиваются все справочники для расшифровки ID
        data = self.get_data(function="getSwitchingByNumber", params=params)
        if not data or not isinstance(data, dict):
            return []
        
        switchingStatusList = switchingStatusList or {row.get('statusID'): row.get('statusName') for row in self.getSwitchingStatusList()}
        switchingTypeList = switchingTypeList or {row.get('switchingTypeID'): row.get('typeName') for row in self.getSwitchingTypeList()}
        switchingCauseList = switchingCauseList or {row.get('switchingCauseID'): row.get('switchingCauseName') for row in self.getSwitchingCauseList()}
        heatingSupplyImpactList = heatingSupplyImpactList or {row.get('heatingSupplyImpactID'): row.get('heatingSupplyImpactName') for row in self.getHeatingSupplyImpactList()}
        companyTZOffset = companyTZOffset or {row.get('companyID'): row.get('tzOffset') for row in self.getCompanyList()}
        statusID = data.get('lastStatusID')
        switchingTypeID = data.get('switchingTypeID')
        switchingCauseID = data.get('switchingCauseID')
        heatingSupplyImpactID = data.get('heatingSupplyImpactID')
        companyID = data.get('companyID')
        startDate = self.datetimeTZOffset(data.get('startDate'), companyID, companyTZOffset)
        endDate = self.datetimeTZOffset(data.get('endDate'), companyID, companyTZOffset)

        # каждому дому присваиваются общие данные заголовка
        header_data = {
            'switchingNumber': data.get('switchingNumber'),
            'statusID': statusID,
            'statusName': switchingStatusList.get(statusID),
            'switchingTypeID': switchingTypeID,
            'switchingTypeName': switchingTypeList.get(switchingTypeID),
            'switchingCauseID': switchingCauseID,
            'switchingCauseName': switchingCauseList.get(switchingCauseID),
            'heatingSupplyImpactID': heatingSupplyImpactID,
            'heatingSupplyImpactName': heatingSupplyImpactList.get(heatingSupplyImpactID),
            'startDate': startDate,
            'endDate': endDate,
            'locality': data.get('locality'),
            'companyID': companyID
        }
        
        # возвращает «плоский» список словарей, где каждая строка — это отдельный адрес с информацией об отключении
        consumers_list = data.get('consumersList')
        if not isinstance(consumers_list, list):
            return []
        
        _target_list = []
        for consumer in consumers_list:
            if isinstance(consumer, dict):
                flat_row = {**consumer, **header_data}
                _target_list.append(flat_row)
                
        return _target_list
    
    # для работы в Airflow
    def getSwitchingData(self, **kwargs):
        periodStartDate = kwargs.get('periodStartDate')
        periodEndDate = kwargs.get('periodEndDate')

        # список изменений за период
        data = self.getSwitchingChangesForPeriod(periodStartDate=periodStartDate, periodEndDate=periodEndDate) or []
        # интеграция с логирование Airflow
        self.log.info(f"Количество полученных записей необработанных изменений: {len(data)}")

        # фильтрация дубликатов
        unique_numbers = {row['switchingNumber'] for row in data if row and row.get('switchingNumber')}
        self.log.info(f"Количество уникальных переключений для обработки: {len(unique_numbers)}")

        self.log.info("Подготовка и кэширование словарей справочников...")
        cached_status = {row.get('statusID'): row.get('statusName') for row in self.getSwitchingStatusList()}
        cached_types = {row.get('switchingTypeID'): row.get('typeName') for row in self.getSwitchingTypeList()}
        cached_causes = {row.get('switchingCauseID'): row.get('switchingCauseName') for row in self.getSwitchingCauseList()}
        cached_impacts = {row.get('heatingSupplyImpactID'): row.get('heatingSupplyImpactName') for row in self.getHeatingSupplyImpactList()}
        cached_tz = {row.get('companyID'): row.get('tzOffset') for row in self.getCompanyList()}

        _target_list_builds = []
        fetch_detail_func = self.getSwitchingByNumber

        def fetch_one_detail(num):
            try:
                return fetch_detail_func(
                    switchingNumber=num,
                    switchingStatusList=cached_status,
                    switchingTypeList=cached_types,
                    switchingCauseList=cached_causes,
                    heatingSupplyImpactList=cached_impacts,
                    companyTZOffset=cached_tz
                ) or []
            except Exception as e:
                self.log.error(f"Ошибка при детальном запросе отключения {num}: {e}")
                return []

        self.log.info("Запуск многопоточного скачивания детальных данных из API...")    
        with ThreadPoolExecutor(max_workers=40) as executor:
            results = executor.map(fetch_one_detail, unique_numbers)

            for task_result in results:
                _target_list_builds.extend(task_result)
        
        self.log.info(f"Многопоточный сбор завершен. Всего собрано плоских строк: {len(_target_list_builds)}")
        return _target_list_builds