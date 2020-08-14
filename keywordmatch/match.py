from tqdm import tqdm
import flashtext
import logging
import os
import pandas as pd


class MatchingProcessor(object):
    """MatchingProcessor Class
        Keyword Matching Using flashtext

    """

    def __init__(self, dataframe, input_column, output_columns):
        """
        Args:
            dataframe : pd.DataFrame
                DataFrame that you want to match keyword
            input_column : string
                String about column name of DataFrame
            output_columns : list
                List about column what you want to match

        """
        self._dataframe = dataframe
        self._input_column = input_column
        self._keyword_dict = {}
        self._keyword_processor = flashtext.KeywordProcessor()
        self._logger = logging.getLogger()
        self._output_columns = output_columns
        # Check variable type
        if type(self._dataframe) != pd.DataFrame:
            raise TypeError(f'You must set dataframe is pd.DataFrame. current type is {type(self._dataframe)}')
        if type(self._input_column) != str:
            raise TypeError(f'You must set input_column is str. current type is {type(self._input_column)}')
        if type(self._output_columns) != list:
            raise TypeError(f'You must set output_columns is list. current type is {type(self._output_columns)}')


    def set_logger(self, logfile_name, is_file = True):
        """To set logger

        Args:
            logfile_name : string
                word that you want to set logfile name

            is_file : bool
                True

        """
        self._logger.setLevel(logging.INFO)
        formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
        file_max_bytes = 10 * 1024 * 1024
        # set StreamHandler
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self._logger.addHandler(stream_handler)

        # set RotatingFileHandler
        if is_file:
            rotating_file_handler = logging.handlers.RotatingFileHandler(filename=os.getcwd()+"/"+logfile_name+".log",
                                                                         maxBytes=file_max_bytes, backupCount=2)
            rotating_file_handler.setFormatter(formatter)
            self._logger.addHandler(rotating_file_handler)


    def add_column(self):
        """To Add columns for matching keyword
        """
        for col in self._output_columns:
            self._dataframe[col] = 0
        self._logger.info(f'Finished Adding Columns: {self._output_columns}')
    
    
    def get_keyword_processor(self, dataframe):
        """To get keyword_processor using flashtext package

        Args:
            dataframe : pd.DataFrame
                DataFrame that you want to match keyword
        """
        for col in dataframe.columns:
            self._keyword_dict[col] = dataframe[col].values.tolist()
        
        self._keyword_processor.add_keywords_from_dict(self._keyword_dict)
        self._logger.info(f'Finished Setting Keyword_processor: {self._keyword_processor.get_all_keywords()}')
    
    
    def is_keyword(self):
        """Match keyword about added column

        Returns:
            dataframe : pd.DataFrame
                DataFrame after keyword matching

        """
        self._logger.info('Start Keyword Match..')
        for i in tqdm(self._dataframe.index):
            text = self._dataframe.at[i, self._input_column]
            extract_keywords = set(self._keyword_processor.extract_keywords(text))
            for keyword in self._output_columns:
                if keyword in extract_keywords:
                    self._dataframe.at[i, keyword] = 1
        self._logger.info('Finished Keyword Match..')
        return self._dataframe


    def save_output_file(self, file_name):
        """To save DataFrame as file
        """
        self._dataframe.to_csv(os.getcwd() + '/' + file_name, mode=a, header=True)
        self._logger.info(f'Saved file {file_name} in {os.getcwd()}')
