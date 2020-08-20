# KeywordMatch
This module can be used to find keywords in Dataframe column from sentences. It is based on the FlashText Package.

# How to Use
```bash
$ git clone https://github.com/jx2lee/keywordmatch.git
$ pip install -r requirements.txt
$ mv keywordmatch {your_python_sys_path}
```

# Usage
***Set Logger***  
```python
>>> from keywordmatch import MatchingProcessor
>>> match_processor = MatchingProcessor(data=df, input_column='기사내용', ouput_columns=['주택', '금융'])
>>> match_processor.set_logger(logfile_name='test', is_file=True) # if is_file is False, Don't save log file.
```  

***Add Column***  
```python
>>> match_processor._data.shape
>>> # (8468, 10)
>>> match_processor.add_column()
>>> # (8468, 12)
```

***Get KeywordProcessor***  
```python
>>> match_processor.get_keyword_processor(data=df_keyword, key='타입', value='키워드')
>>> # [2020-08-20 12:17:47,595][INFO] Finished Setting Keyword_processor: {'중랑구': '주택', '부산': '주택', '경남': '주택', '은행': '금융', '송금': '금융', '출금': '금융'}
>>> # key 	: DataFrame column for type of keyword
>>> # value	: DataFrame column for value of keyword
```

***Match Keyword***  
```python
>>> result = match_processor.is_keyword()
>>> # [2020-08-20 12:21:07,900][INFO] Start Keyword Match.
>>> # 100%|██████████| 8468/8468 [00:05<00:00, 1521.65it/s]
>>> # [2020-08-20 12:21:13,483][INFO] Finished Keyword Match.
```

***Save to file***  
```python
>>> match_processor.save_output_file(file_name='test.csv')
>>> # [2020-08-20 12:27:54,763][INFO] Finished Saving file test.csv in /Users/jj/python/coding-test
```

***Save to Database (Tibero)***  
```python
>>> connection_info = {'ip':'192.168.179.166',
          	  		   'port':'8629',
          	  		   'sid':'tibero',
          	  		   'id_pw':['{user_id}', '{user_pw}'],
          	  		   'output_columns':['기사제목','기사내용','수집시간'],
          	  		   'table': 'CRAWLER_DATA',
          	  		   'table_columns':['DETECTED_LINK', 'DETECTED_CONTENTS', 'DETECTED_TIME']}
>>> match_processor.save_output_database(jar_file='{your_path_for_tibero_jar}', db_info=connection_info)
>>> # [2020-08-20 13:36:34,415][INFO] Connected Tibero: 192.168.179.166:8629:tibero
>>> # [2020-08-20 13:36:34,463][INFO] Started Creating SQL dump.
>>> # 100%|██████████| 8468/8468 [00:00<00:00, 44935.09it/s]
>>> # [2020-08-20 13:36:34,659][INFO] Finished Creating SQL dump. dump size: 8468
>>> # [2020-08-20 13:36:34,660][INFO] Started pushing data. SQL Query: INSERT INTO CRAWLER_DATA VALUES (?,?,?)
>>> # [2020-08-20 13:36:36,272][INFO] Finished pushing data.
>>> # [2020-08-20 13:36:36,273][INFO] Disconnected Tibero.
```

# Test
```bash
$ git clone https://github.com/jx2lee/keywordmatch.git
$ pip install -r requirements.txt
$ mv keywordmatch {your_python_sys_path}
$ python example.py # edit connection_info to your DB Setting value.
```

# Benchmark
This repository was created by referring to [FlashText](https://github.com/vi3k6i5/flashtext) Python package.

---
made by *jaejun.lee*  
jaejun.lee.1991@gmail.com