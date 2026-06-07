import uuid  
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR as String, BOOLEAN, UUID, DATE, TIMESTAMP
from .base import ODSEntity, Column, Tie
from hooks.mpts_hook import MPTSHooks

class MPTSSwitchingODS(ODSEntity):
    # динамический сбор списка всех колонок
    columns = []  
    bk = 'sid'
    increment_flg = True

    sid = Column(type=String, primary_key=True)
    # d_day_id = Column(type=String, primary_key=True)    
    
    zuluObjectID = Column(type=String, change_type='static')
    # guid_link = Tie(entity='building', pk='sid', type=String) # связка Tie по pk='guid'
    build_guid = Column(type=UUID, change_type='static')
    geometryCoords = Column(type=String, change_type='static')
    buildingTypeID = Column(type=INTEGER, change_type='static')
    disabled = Column(type=BOOLEAN, change_type='static')
    gvsDisabled = Column(type=BOOLEAN, change_type='static')
    osDisabled = Column(type=BOOLEAN, change_type='static')
    ventDisabled = Column(type=BOOLEAN, change_type='static')
    switchingNumber = Column(type=INTEGER, change_type='static')
    statusName = Column(type=String, change_type='static')
    switchingTypeName = Column(type=String, change_type='static')
    switchingCauseName = Column(type=String, change_type='static')
    heatingSupplyImpactName = Column(type=String, change_type='static')
    startDate = Column(type=TIMESTAMP, change_type='static')
    endDate = Column(type=TIMESTAMP, change_type='static')
    locality = Column(type=String, change_type='static')
    companyID = Column(type=INTEGER, change_type='static')
    
def switching(**kwargs):
    execution_date = kwargs.get('date') or datetime.today() # извлекаем дату выполнения
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    if hasattr(execution_date, 'date'):
        execution_date = execution_date.date()

    # расчет периодов загрузки
    start_period = execution_date + relativedelta(days=1)
    end_period = execution_date + relativedelta(days=2)

    if isinstance(start_period, str):
        start_period = datetime.strptime(start_period, "%Y-%m-%d %H:%M:%S")
    if isinstance(end_period, str):
        end_period = datetime.strptime(end_period, "%Y-%m-%d %H:%M:%S")

    # print(f"TEST - {kwargs}")
    print(f"Начата операция получения данных переключения за указанный период: {start_period.strftime('%Y-%m-%d')} - {end_period.strftime('%Y-%m-%d')}")

    # безопасная инициализация хука
    api_hook = MPTSHooks(mpts_conn_id='mpts_api_prod')
    raw_data = api_hook.getSwitchingData(
        periodStartDate=start_period.strftime("%Y-%m-%d %H:%M:%S"),
        periodEndDate=end_period.strftime("%Y-%m-%d %H:%M:%S")
    )

    db_columns = [
        'sid', 'zuluObjectID', 'guid_link', 'build_guid', 'geometryCoords', 'buildingTypeID', 
        'disabled', 'gvsDisabled', 'osDisabled', 'ventDisabled', 'switchingNumber', 
        'statusName', 'switchingTypeName', 'switchingCauseName', 'heatingSupplyImpactName', 
        'startDate', 'endDate', 'locality', 'companyID'
    ]

    if not raw_data:
        print("Не получены данные из API MPTS за указанный период.")
        return pd.DataFrame(columns=db_columns)
    
    # обработка в Pandas
    df = pd.DataFrame(raw_data)
    if 'GUID' in df.columns:
        df = df.rename(columns={'GUID': 'build_guid'})

    df = df.dropna(subset=['switchingNumber', 'zuluObjectID'])
    if df.empty:
        print("После фильтрации dropna не осталось валидных данных.")
        return pd.DataFrame(columns=db_columns) 
    
    df['switchingNumber'] = df['switchingNumber'].astype(int).astype(str)
    df['zuluObjectID'] = df['zuluObjectID'].astype(int).astype(str)

    df['sid'] = df.apply(
        lambda row: str(f"{row['switchingNumber']}_{row['zuluObjectID']}"),
        axis=1
    )

    df['d_day_id'] = start_period.strftime("%Y-%m-%d")
    
    boolean_cols = ['disabled', 'gvsDisabled', 'osDisabled', 'ventDisabled']
    for col in db_columns:
        if col not in df.columns:
            df[col] = False if col in boolean_cols else None
            
    df = df.drop_duplicates(subset=[MPTSSwitchingODS.bk], keep='last')

    df = df[db_columns]

    return df