from datetime import datetime
from keywordmatch import MatchingProcessor
import os
import pickle
import pandas as pd

if __name__ == "__main__":
    with open("example.pickle", "rb") as f:
        test_df = pickle.load(f)
    test_df_keyword = pd.DataFrame({'타입':['금융', '주택', '금융', '주택', '금융', '주택'],
                                    '키워드':['은행', '중랑구', '송금', '부산', '출금', '경남']})

    instance = MatchingProcessor(test_df, '기사내용', ['주택', '금융'])
    instance.set_logger('t1', is_file=False)
    instance.add_column()
    instance.get_keyword_processor(test_df_keyword, '타입', '키워드')
    result = instance.is_keyword()

    instance._data['수집시간'] = datetime(2020, 8, 19).strftime('%Y-%m-%d')
    tibero = {'ip': '192.168.179.166',
              'port': '8629',
              'sid': 'tibero',
              'id_pw': ['tibero', 'tmax'],
              'output_columns': ['기사제목', '기사내용', '수집시간'],
              'table': 'CRAWLER_DATA',
              'table_columns': ['DETECTED_LINK', 'DETECTED_CONTENTS', 'DETECTED_TIME']}
    instance.save_output_database(jar_file=os.getcwd() + '/' + 'tibero6-jdbc.jar', db_info=tibero)
