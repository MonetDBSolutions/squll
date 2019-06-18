"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
The sqalpel.yaml file can be inspected for local processing.
Use of pyyaml 5.1 is required for plugging an exploit
"""
import logging
import urllib.parse

import requests
import yaml


class Repository:
    debug = False

    @staticmethod
    def isvalid(url):
        res = True
        try:
            o = urllib.parse.urlparse(url)
            if o.path[-4:] != '.git':
                if Repository.debug:
                    logging.error(f'Missing extension {o.path[-4:]}')
                res = False
        except ValueError as msg:
            if Repository.debug:
                logging.error(f"Invalid url {url} {msg}")
            return False
        if not o.netloc or not o.scheme:
            if Repository.debug:
                logging.error(f"Invalid url {url} on {o.scheme}, {o.netloc}, {o.path}")
            return False
        logging.info(f"Valid url {url}")
        return res

    @staticmethod
    def get_yaml(repro, file):
        logging.info(f'{repro},{file}')
        if not Repository.isvalid(repro):
            return None
        try:
            o = urllib.parse.urlparse(repro)
            if Repository.debug:
                logging.info(f"parsed {o}")
        except ValueError as msg:
            if Repository.debug:
                logging.error(f'Invalid yaml url {file} {msg}')
            return None
        url = f"https://raw.githubusercontent.com{o.path[:-4]}/master/{file}"
        if Repository.debug:
            logging.info(f"URL retrieved {url}")
        result = requests.get(url)
        if result:
            if Repository.debug:
                logging.info(f"result{result.text}")
            try:
                p = yaml.load(result.text)
            except yaml.YAMLError as msg:
                logging.error(msg)
                return None
            return p
        if Repository.debug:
            logging.info(f"FAILED {result} {url}")
        return None

    @staticmethod
    def parse_yaml(repro, file):
        logging.info(f"{repro},{file}")
        try:
            o = urllib.parse.urlparse(repro)
            if Repository.debug:
                logging.info(f"parsed {o}")
        except yaml.YAMLError as msg:
            if Repository.debug:
                logging.error(f'Invalid yaml url {file} {msg}')
            return {'parse_error': msg}
        url = f"https://raw.githubusercontent.com{o.path[:-4]}/master/{file}"
        if Repository.debug:
            logging.info(f"URL retrieved {url}")
        result = requests.get(url)
        if result:
            if Repository.debug:
                logging.info(f"result{result.text}")
            try:
                p = yaml.load(result.text)
            except yaml.YAMLError as msg:
                logging.error(msg)
                return {'parse_error': msg}
            return p
        if Repository.debug:
            logging.info(f"FAILED {result} {url}")
        return None

    @staticmethod
    def get_documentation(repro):
        if not repro:
            return None

        try:
            o = urllib.parse.urlparse(repro)
            if Repository.debug:
                logging.info(f"parsed {o}")
        except ValueError as msg:
            if Repository.debug:
                logging.error(f'Invalid yaml url {repro} {msg}')
            return None
        y = Repository.parse_yaml(repro, 'sqalpel.yaml')
        if not y:
            return None
        if y['documentation']:
            url = f"https://raw.githubusercontent.com{o.path[:-4]}/master/{y['documentation']}"
            if Repository.debug:
                logging.info(f"Retrieve documentation from url {url}")
            result = requests.get(url)
            if result:
                if Repository.debug:
                    logging.info(result.text)
                return result.text
        return None

    @staticmethod
    def get_experiments(repro):
        if not repro:
            return None
        y = Repository.parse_yaml(repro, 'sqalpel.yaml')
        if not y:
            return None
        if 'experiments' not in y:
            logging.error(f'Invalid yaml url {repro}, missing experiments section')
            return None
        try:
            o = urllib.parse.urlparse(repro)
            if Repository.debug:
                logging.info(f"{o}")
        except ValueError as msg:
            return {'parse_error': msg}
        if type(y['experiments']) is not list:
            y = Repository.parse_yaml(repro, y['experiments'])
            if y and 'parse_error' not in y:
                if Repository.debug:
                    logging.info(f"FOUND experiments")
                    return y
            else:
                return {'parse_error': y}
        else:
            return y
        return None

    @staticmethod
    def get_databases(repro):
        if not repro:
            return None
        y = Repository.parse_yaml(repro, 'sqalpel.yaml')
        if not y:
            return None
        if 'databases' not in y:
            logging.error(f'Invalid yaml url {repro}, missing databases section')
            return None
        try:
            o = urllib.parse.urlparse(repro)
            if Repository.debug:
                logging.info(f"{o}")
        except ValueError:
            return None
        if type(y['databases']) is not list:
            y = Repository.parse_yaml(repro, y['databases'])
            if y and 'parse_error' not in y:
                if Repository.debug:
                    logging.info(f"FOUND experiments")
                return y
            else:
                return {'parse_error': y}
        else:
            return y
