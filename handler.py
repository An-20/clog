"""
aureus.aureus_utils.consolidated_logger.handler

Contains the core code for ConsolidatedHandler
"""

import os
import gzip
import time
import logging
import pathlib
import datetime


class ConsolidatedHandler(logging.Handler):
    """
    A logging handler which is compatible with the Python stdlib logging library.

    """

    def __init__(
            self,
            base_log_directory: str,
            archive_threshold_days: int = 30
    ) -> None:
        """
        __init__() magic method for ConsolidatedHandler.

        :param base_log_directory: The base log directory (contains archived and unarchived directories)
        :type base_log_directory: str

        :param archive_threshold_days: The number of days before logs are archived
        :type archive_threshold_days: int

        :return None
        :rtype NoneType
        """
        super(ConsolidatedHandler, self).__init__()

        self.base_log_directory: str = base_log_directory
        self.base_archived_log_directory: str = f"{base_log_directory}/archived/"
        self.base_unarchived_log_directory: str = f"{base_log_directory}/unarchived/"

        self.archive_threshold_days: int = archive_threshold_days

    def archive_necessary(self) -> None:
        """
        Archives the necessary logs

        :return:
        """
        archive_threshold = (
            datetime.datetime.now() -
            datetime.timedelta(days=self.archive_threshold_days)
        )

        for filename in os.listdir(self.base_unarchived_log_directory):
            log_datetime_formatted = pathlib.Path(filename).stem
            log_datetime = datetime.datetime.strptime(
                log_datetime_formatted,
                "%Y-%m-%d"
            )
            if log_datetime <= archive_threshold:
                # archive the log
                new_filename = f"{log_datetime_formatted}.AU_LOG"
                old_log_filepath = os.path.join(self.base_unarchived_log_directory, new_filename)
                new_log_filepath = os.path.join(self.base_archived_log_directory, new_filename)
                new_log_filepath += ".gz"
                if os.path.exists(new_log_filepath):
                    new_log_filepath = f"{new_log_filepath}_X"
                    if os.path.exists(new_log_filepath):
                        os.remove(new_log_filepath)
                with open(old_log_filepath, "r") as file:
                    log_data = file.read()
                os.remove(old_log_filepath)
                with gzip.open(new_log_filepath, "xb") as file:
                    file.write(log_data.encode("utf-8"))

    def get_log_filepath(
            self,
            log_timestamp: float
    ) -> str:
        """
        Gets log filepath with the given log timestamp

        :param log_timestamp: The log timestamp
        :type log_timestamp: float

        :return: Returns a full filepath from the unarchived folder
        :rtype: str
        """
        datetime_formatted = datetime.datetime.fromtimestamp(
            log_timestamp).strftime("%Y-%m-%d")
        filename = f"{datetime_formatted}.AU_LOG"
        return f"{self.base_unarchived_log_directory}/{filename}"

    def emit(
            self,
            record: logging.LogRecord
    ) -> None:
        text_to_write = self.format(record) + "\n"
        log_filepath = self.get_log_filepath(time.time())

        if not os.path.exists(log_filepath):
            with open(log_filepath, "x") as file:
                file.write(text_to_write)
        else:
            with open(self.get_log_filepath(time.time()), "a") as file:
                file.write(text_to_write)
        self.archive_necessary()
