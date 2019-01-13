"""
This module contains a number of utility functions.
"""
from contextlib import contextmanager
from datetime import datetime

import json
import logging
import logging.handlers
import os
import platform
import requests


DATE_FORMAT = "%Y-%m-%d"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name, log_file_name, log_rotate_size=None, log_rotate_count=10):
    """
    Creates a logger identified by name that logs to log_file_name. The logger
    would be set in DEBUG level.
    Example:
        logger = get_logger(__name__, '/var/log/log1.log')
        logger.debug('This is a debug level log.')
        logger.info('This is an info level log.')
        logger.warning('This is an warning level log.')
        logger.critical('This is an critical level log.')

    :param name: str - name of logger.
    :param log_file_name: str - path to log file.
    :param log_rotate_size: int - max size of rotate log files in bytes.
    :param log_rotate_count: int - max number of rotate log files to keep.
    :return: logger object.
    """
    # Check if log file exits with write access.
    if not os.path.exists(log_file_name):
        print("Cannot find log file '{}'. Check that it exists.".format(log_file_name))
        return
    if not os.access(log_file_name, os.W_OK):
        msg = "Cannot get WRITE access to log file '{}'. Change file permissions, "
        msg += "then try again."
        print(msg.format(log_file_name))
        return
    # Get logger.
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if log_rotate_size:
        # Create rotate file handler
        log_handler = logging.handlers.RotatingFileHandler(
            log_file_name, maxBytes=log_rotate_size, backupCount=log_rotate_count)
    else:
        # Create file handler.
        file_handler = logging.FileHandler(log_file_name)

    file_handler.setLevel(logging.DEBUG)
    # Format logging.
    log_formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(levelname)-6s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(log_formatter)
    # Now add file hander to logger.
    logger.addHandler(file_handler)

    return logger


def get_sys_logger():
    """
    Setup logging.
    :return - return logger.
    """
    log_file = os.path.splitext(__file__)[0]

    # Create logger
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)

    # Ensure that it is logged to syslog
    log_address = '/dev/log'
    if platform.system() == 'Darwin':
        log_address = '/var/run/syslog'
    handler = logging.handlers.SysLogHandler(address=log_address)
    logger.addHandler(handler)
    return logger


def send_slack_notification(webhook_url, username, **kwargs):
    """
    Send slack notification.

    :param webhook_url: string - url to send to. Slack webhook url.
    :param username: string - sender
    """
    response = requests.post(
        url=webhook_url,
        data=json.dumps({"username": username, "attachments": [kwargs]}),
        headers={'Content-type': 'application/json'}
    )
    if response.status_code != 200:
        logger = get_sys_logger()
        logger.error("{} - Could not send slack notification.".format(__file__))


def format_timedelta(time_delta, with_micro_secs=False):
    """
    Timedelta formatter for hours:mins:seconds

    :param td: object - datatime.timedelta object.
    :param with_micro_secs: boolean - indicating whether microseconds should be displayed.
    :return: string time formatted as "hours:mins:sec:msec" when with_micro_secs in True, 
        "hours:mins:sec" otherwise.
    """
    minutes, seconds = divmod(time_delta.seconds + time_delta.days * (60 * 60 * 24), 60)
    hours, minutes = divmod(minutes, 60)
    if with_micro_secs:
        return '{:d}:{:02d}:{:02d}.{:06d}'.format(
            hours, minutes, seconds, time_delta.microseconds)
    return '{:d}:{:02d}:{:02d}'.format(hours, minutes, seconds)


@contextmanager
def timer(with_duration=False):
    """
    Context manager to print out start and end times.
    :param with_duration: boolean indicating whether duration should be printed.
    """
    try:
        start_time = datetime.now()
        print("Start Time: {}".format(start_time.strftime(DATE_TIME_FORMAT)))
        yield
    finally:
        end_time = datetime.now()
        print("\nEnd Time: {}".format(end_time.strftime(DATE_TIME_FORMAT)))
        if with_duration:
            print("Duration: {}".format(format_timedelta(end_time-start_time)))
