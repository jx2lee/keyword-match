from datetime import datetime
from tqdm import tqdm
import flashtext
import jaydebeapi
import logging
import logging.handlers
import numpy
import os
import pandas as pd


def _set_log(has_file = False):

    logger = logging.getLogger('keywordmatch')
    logger.propagate = False

    # Check Variable type
    assert type(has_file) == bool, f'You must set input_column is boolean. Current type is {has_file.__class__.__name__}'

    # Check Logger handler exists
    if len(logger.handlers) >= 2:
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
    file_max_bytes = 10 * 1024 * 1024

    # set StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # set RotatingFileHandler
    if has_file:
        rotating_file_handler = logging.handlers.RotatingFileHandler(
            filename=os.getcwd() + "/" + logfile_name + ".log",
            maxBytes=file_max_bytes,
            backupCount=2)
        rotating_file_handler.setFormatter(formatter)
        logger.addHandler(rotating_file_handler)

    return logger


class MatchingProcessor(object):
    """MatchingProcessor Class
        Keyword Matching Using FlashText

    """

    def __init__(self, data, input_column, keyword_category, has_log_file = False):
        """
        Args:
            data : pd.DataFrame
                DataFrame that you want to match keyword
            input_column : string
                String about column name of DataFrame
            keyword_category : list
                List about column what you want to match

        """

        # Check Variable type
        assert type(data) == pd.DataFrame, f'You must set data is pd.DataFrame. Current type is {data.__class__.__name__}'
        assert type(input_column) == str, f'You must set input_column is str. Current type is {input_column.__class__.__name__}'
        assert type(keyword_category) == list, f'You must set keyword_category is list. Current type is {keyword_category.__class__.__name__}'

        self._data = data
        self._input_column = input_column
        self._keyword_dict = {}
        self._keyword_processor = flashtext.KeywordProcessor()
        self._keyword_categroy = keyword_category
        self._logger = _set_log(has_file = has_log_file)

    def add_column(self):
        """To Add columns for matching keyword. Default new value is 0. (each row)

        """

        for col in self._keyword_categroy:
            self._data[col] = '0'
        self._logger.info(f'Finished Adding Columns: {self._keyword_categroy}')

    def get_keyword_processor(self, data, key, value):
        """To get keyword_processor using FlashText

        Args:
            data : pd.DataFrame
                DataFrame with keywords (master table)

            key : string
                DataFrame Column for Keyword type

            value : string
                DataFrame Column for Keyword value

        """

        # Check Variable type
        assert type(data) == pd.DataFrame, f'You must set data is pd.DataFrame. Current type is {data.__class__.__name__}'
        assert type(key) == str, f'You must set key is str. Current type is {key.__class__.__name__}'
        assert type(value) == str, f'You must set value is str. Current type is {value.__class__.__name__}'

        for col in self._keyword_categroy:
            self._keyword_dict[col] = data[data[key] == col][value].to_list()

        self._keyword_processor.add_keywords_from_dict(self._keyword_dict)
        self._logger.info(f'Finished Setting Keyword_processor')

    def is_keyword(self):
        """Match keyword about added column

        Returns:
            data : pd.DataFrame
                DataFrame after keyword matching

        """
        self._logger.info('Start Keyword Match.')
        for i in tqdm(self._data.index):
            text = self._data.at[i, self._input_column]
            extract_keywords = set(self._keyword_processor.extract_keywords(text))
            for keyword in self._keyword_categroy:
                if keyword in extract_keywords:
                    self._data.at[i, keyword] = '1'
        self._logger.info(f'Finished Keyword Match.')
        return

    def save_output_file(self, file_name):
        """To save DataFrame as file

        Args:
            file_name : string

        """

        # Check Variable type
        assert type(file_name) == str, f'You must set key is string. Current type is {file_name.__class__.__name__}'

        # Save to CSV
        self._data.to_csv(os.getcwd() + '/' + file_name, mode='a', header=True)
        self._logger.info(f'Finished Saving file {file_name} in {os.getcwd()}')

    def save_output_database(self, jar_file, db_info):
        """To save DataFrame in Tibero

        Args:
            jar_file : string
                jar file path for Tibero connection

            db_info : dictionary
                info for Tibero Connection. For example:
                *ip/port/sid(str)       : ip address/port/sid for Tibero
                *id_pw(str)             : User ID/Password
                *table(str)             : Table to be loaded
                *table_columns(list)    : Columns to save
                *output_columns(list)   : Columns to be extracted from the data frame

        Examples:
            >>> db = {"ip" : "192.168.179.166", 'sid':'tibero', "id_pw":['tibero', 'tmax'],
            >>> 'output_columns':['기사제목','기사내용','수집시간'], 'table': 'CRAWLER_DATA',
            >>> 'table_columns':['DETECTED_LINK', 'DETECTED_CONTENTS', 'DETECTED_TIME']}
            >>> instance.save_output_database(jar_file='{your_jar_file_path}/tibero6-jdbc.jar', db_info=db)

        """
        # Check jar file
        if not os.path.isfile(jar_file):
            raise IOError(f'Invalid file path {jar_file}')

        # Check length table_columns & output_columns
        assert len(db_info['output_columns']) == len(db_info['table_columns']), f'Set equal length (output_colums, table_columns)'

        # Database Connection
        conn = jaydebeapi.connect('com.tmax.tibero.jdbc.TbDriver',
                                  f'jdbc:tibero:thin:@{db_info["ip"]}:{db_info["port"]}:{db_info["sid"]}',
                                  db_info['id_pw'],
                                  jar_file)
        self._logger.info(f'Connected Tibero: {db_info["ip"]}:{db_info["port"]}:{db_info["sid"]}')

        # Create data dump during Loop & SQL qeury
        self._logger.info(f'Creating SQL dump.')
        dump = []
        np_type_list = (numpy.int8, numpy.int16, numpy.int32, numpy.int64)
        for i in self._data.index:
            dump.append(list(self._data.at[i, col] if type(self._data.at[i, col]) not in np_type_list else int(self._data.at[i, col]) for col in db_info['output_columns']))

        alter_query = f'ALTER SESSION SET NLS_DATE_FORMAT = \'YYYY/MM/DD HH24:MI:SS\''
        insert_query = f'INSERT INTO {db_info["table"]} VALUES ({",".join(str("?") for i in range(len(db_info["table_columns"])))})'
        count_query = f'SELECT COUNT(*) FROM {db_info["table"]}'

        self._logger.info(f'Finished Creating SQL dump. dump size: {len(dump)}')
        self._logger.info(f'Finished Creating SQL query. alter_query:{alter_query}, insert_query:{insert_query}')

        # Set Cursor and Put dump
        cursor = conn.cursor()
        if not dump:
            self._logger.warn(f'Empty sql dump. Skip Inserting data to table')
            return
        elif len(dump) > 1:
            cursor.execute(alter_query)
            cursor.executemany(insert_query, dump)
        else:
            cursor.execute(alter_query)
            cursor.execute(insert_query, dump)

        # Count Rows
        cursor.execute(count_query)
        self._logger.info(f'Finished pushing data. # of rows: {cursor.fetchall()[0][0]}')

        # Disconnect Database
        conn.commit()
        conn.close()
        self._logger.info(f'Disconnected Tibero.')
