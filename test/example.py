from datetime import datetime
import os
import pickle
import pandas as pd

if __name__ == "__main__":
    import sys

    run_path = os.getcwd()
    sys.path.append(run_path)
    from keywordmatch import MatchingProcessor
    

    with open(f'{run_path}/test/example.pickle', "rb") as f:
        test_df = pickle.load(f)
    test_df_keyword = pd.DataFrame({'타입':['금융', '주택', '금융', '주택', '금융', '주택'],
                                    '키워드':['은행', '중랑구', '송금', '부산', '출금', '경남']})


    instance = MatchingProcessor(test_df, '기사내용', ['주택', '금융'])
    instance.set_logger('t1', is_file=False)
    instance.add_column()
    instance.get_keyword_processor(test_df_keyword, '타입', '키워드')
    result = instance.is_keyword()

    instance._data['수집시간'] = datetime(2020, 9, 14).strftime('%Y-%m-%d')
    tibero_info = {'ip': '{database_ip}',
              'port': '{database_port}}',
              'sid': '{database_sid}',
              'id_pw': ['{id}', '{password}'],
              'output_columns': ['기사제목', '기사내용', '주택', '금융', '수집시간'],
              'table': '{table_name}',
              'table_columns': ['DETECTED_LINK', 'DETECTED_CONTENTS', 'IS_FINANCE', 'IS_HOUSE', 'DETECTED_TIME']}
    instance.save_output_database(jar_file=f'{run_path}/lib/tibero6-jdbc.jar', db_info=tibero_info)