import requests
import json
import time, datetime
from logging import getLogger, StreamHandler, FileHandler, Formatter, DEBUG, INFO

HEADERS = {'content-type': 'application/json'}

# Paging parameter for list
PAGE_LIMIT = 20
MAX_ELEMENTS = 100000

# Wait interval
WAIT_SECONDS = 10

def get_property(dic, key):
    if key in dic:
        return str(dic[key])
    else:
        return '(no ' + key + ')'

class InsightDatabaseTesting():
    def __init__(self, url_base, user, password, upper_logger = None):
        """
        Create Insight Database Testing session.

        Parameters
        ----------
        url_base : string
            URL to Insight DM Manager
            example: http://127.0.0.1:7777/
        user : user name
        passwoer : password for the user
        upper_logger : logger to be used
        """
        self._logger = upper_logger or getLogger(__name__)

        self._url_base = url_base + 'api/v2/'
        self._cookies = self._create_session(user, password)

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, trace):
        self._remove_session()

    def __del__(self):
        self._remove_session()

    def _create_session(self, user, password):
        url = self._url_base + 'auth'
        body = { 'username': user, 'password': password }
        r = requests.post(url, headers=HEADERS, json=body)
        self._logger.info(r.json())
        return r.cookies

    def _remove_session(self):
        if self._cookies is not None:
            url = self._url_base + 'auth'
            r = requests.delete(url, headers=HEADERS, cookies=self._cookies)
            self._logger.info(r.json())
            self._cookies = None


    def _call_api(self, method, api, body=None, files=None):
        url = self._url_base + api
        if method == 'GET':
            r = requests.get(url, headers=HEADERS, cookies=self._cookies)
        elif method == 'POST':
            r = requests.post(url, headers=HEADERS, json=body, cookies=self._cookies)
        elif method == 'POST_UPLOAD':
            r = requests.post(url, files=files, data=body, cookies=self._cookies)
        elif method == 'PUT':
            r = requests.put(url, headers=HEADERS, json=body, cookies=self._cookies)
        elif method == 'DELETE':
            r = requests.delete(url, headers=HEADERS, json=body, cookies=self._cookies)
        else:
            error_message = 'Unknown method:' + method + 'for api:' + api + '.'
            self._logger.error(error_message)
            raise ValueError(error_message)

        if r.status_code != 200:
            self._logger.error('status=' + str(r.status_code))
            self._logger.error('text=' + r.text)
            return None

        self._logger.debug(json.dumps(r.json(), indent=2))

        return r.json()

    def _list_elements(self, list_key):
        elements = []
        for offset in range(0, MAX_ELEMENTS, PAGE_LIMIT):
            response = self._call_api('GET', list_key + '?limit=' + str(PAGE_LIMIT) + '&offset=' + str(offset))
            if len(response['rows']) == 0:
                break

            # search id from name
            for target_element in response['rows']:
                elements.append(target_element.copy())

        return elements

    def _get_id_from_name(self, list_key, target_name):
        for offset in range(0, MAX_ELEMENTS, PAGE_LIMIT):
            response = self._call_api('GET', list_key + '?limit=' + str(PAGE_LIMIT) + '&offset=' + str(offset))
            if len(response['rows']) == 0:
                # not found
                return None

            # search id from name
            target_id = '-'
            for target_element in response['rows']:
                if target_element['name'] == target_name:
                    target_id = target_element['id']
                    return target_id

    # for Database operation

    def list_databases(self):
        return self._list_elements('databases')

    def get_database_id(self, database_name):
        return self._get_id_from_name('databases', database_name)

    # for SQL workload operation

    def list_sql_workloads(self):
        return self._list_elements('sql-workloads')
    
    def print_sql_workloads(self):
        sql_workloads = self.list_sql_workloads()

        for sql_workload in sql_workloads:
            print('SQL-workload: ' + get_property(sql_workload, 'name') + ' (' + get_property(sql_workload, 'id') + ')')
            if 'jobs' in sql_workload:
                if len(sql_workload['jobs']) > 0:
                    if 'startTime' in sql_workload['jobs'][0]:
                        print('    start:  ' + sql_workload['jobs'][0]['startTime'])
                    else:
                        print('    start:  no start date info.')
                    if 'endTime' in sql_workload['jobs'][0] and sql_workload['jobs'][0]['endTime'] is not None:
                        print('    finish: ' + sql_workload['jobs'][0]['endTime'])
                        if 'startTime' in sql_workload['jobs'][0]:
                            print('    elapsed time: ' + str(datetime.datetime.strptime(sql_workload['jobs'][0]['endTime'], '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.datetime.strptime(sql_workload['jobs'][0]['startTime'],  '%Y-%m-%dT%H:%M:%S.%fZ')))
                        else:
                            print('    elapsed time: cannot calculate')
                        print('    SQL count: ' + get_property(sql_workload['jobs'][0], 'count'))
                    else:
                        print('    not finished.')
                else:
                    print('    jobs element is empty.')
            else:
                print('    no jobs element.')

    def create_sql_workload(self, sql_workload_name, db_type, source_file_name, is_unique = False):
        self._logger.info('Create SQL-workload: ' + sql_workload_name)
        body = { 'name': sql_workload_name, 'dbType': db_type, 'dataKind': 'MS', 'source': source_file_name, 'unique': ('true' if is_unique else 'false') }
        response = self._call_api('POST', 'sql-workloads/', body)
        if response is None:
            return

        sql_workload_id = response['id']

        time.sleep(WAIT_SECONDS)

        # wait until sql_set is ready
        while True:
            response = self._call_api('GET', 'sql-workloads/' + sql_workload_id)
            if response['status'] == 1:
                self._logger.info('  processed sqls:' + str(response['jobs'][0]['count']))
                break

            if 'jobs' in response and len(response['jobs']) > 0:
                if 'count' in response['jobs'][0] and response['jobs'][0]['count'] is not None:
                    self._logger.info('  current processed sqls:' + str(response['jobs'][0]['count']))
                else:
                    self._logger.warning('  not started ...')
            else:
                self._logger.info('  preparing ...')
            time.sleep(WAIT_SECONDS)

        return response['id']

    def create_sql_workload_upload(self, sql_workload_name, db_type, source_file_path, is_unique = False):
        self._logger.info('Create SQL-workload(upload): ' + sql_workload_name)
        file_content = open(source_file_path, 'rb')
        files = {'source': ('upload_file', file_content, 'text/csv')}
        body = { 'name': sql_workload_name, 'dbType': db_type, 'dataKind': 'MS', 'unique': ('true' if is_unique else 'false') }
        response = self._call_api('POST_UPLOAD', 'sql-workloads/upload', body, files)
        if response is None:
            return

        sql_workload_id = response['id']

        time.sleep(WAIT_SECONDS)

        # wait until sql_set is ready
        while True:
            response = self._call_api('GET', 'sql-workloads/' + sql_workload_id)
            if response['status'] == 1:
                self._logger.info('  processed sqls:' + str(response['jobs'][0]['count']))
                break

            if 'jobs' in response and len(response['jobs']) > 0:
                if 'count' in response['jobs'][0] and response['jobs'][0]['count'] is not None:
                    self._logger.info('  current processed sqls:' + str(response['jobs'][0]['count']))
                else:
                    self._logger.warning('  not started ...')
            else:
                self._logger.info('  preparing ...')
            time.sleep(WAIT_SECONDS)

        return response['id']

    # for Assessment operation

    def list_assessments(self):
        return self._list_elements('assessments')
    
    def print_assessments(self):
        assessments = self.list_assessments()

        for assessment in assessments:
            print('Assessment: ' + assessment['name'] + ' (' + assessment['id'] + ')')
            if 'jobs' in assessment:
                if len(assessment['jobs']) > 0:
                    if 'startTime' in assessment['jobs'][0]:
                        print('    start:  ' + str(datetime.datetime.strptime(assessment['jobs'][0]['startTime'], '%Y-%m-%dT%H:%M:%S.%fZ')))
                    else:
                        print('    start:  no start date info.')
                    if 'endTime' in assessment['jobs'][0] and assessment['jobs'][0]['endTime'] is not None:
                        print('    finish: ' + str(datetime.datetime.strptime(assessment['jobs'][0]['endTime'], '%Y-%m-%dT%H:%M:%S.%fZ')))
                        if 'startTime' in assessment['jobs'][0]:
                            print('    elapsed time:  ' + str(datetime.datetime.strptime(assessment['jobs'][0]['endTime'], '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.datetime.strptime(assessment['jobs'][0]['startTime'], '%Y-%m-%dT%H:%M:%S.%fZ')))
                        else:
                            print('    elapsed time: cannot calculate')

                        response2 = self._call_api('GET', 'assessments/' + assessment['id'] + '/summary')
                        if response2 is not None:
                            if 'all' in response2:
                                print('    success: ' + str(response2['all']['success']) + ', failed: ' + str(response2['all']['failed']))
                            else:
                                print('    no all element')
                        else:
                            print('    cannot get summary')
                    else:
                        print('    not finished.')
                else:
                    print('    jobs element is empty.')
            else:
                print('    no jobs element.')

    def execute_assessment(self, assessment_name, sql_workload_id, target_db_id, db_users, db_user_passwords, concurrency = 1, execMode = 'P', transaction = 'R', etimeZero = 0.2, convertParameter = False, fillingBindValueMap = {'UNKNOWN': { 'type': 'UNKNOWN', 'value':'-'}}):
        self._logger.info('Execute Assessment: ' + assessment_name)
        body = { 'name': assessment_name, 'sqlWorkloadId': sql_workload_id, 'databaseId': target_db_id, 'users': db_users, 'pswds': db_user_passwords, 'concurrency': concurrency, 'execLevel': execMode, 'transaction': transaction, 'start': '00000000000000', 'end': '99999999999999', 'etimeZero': etimeZero, 'convertParameter': convertParameter, 'fillingBindValueMap': fillingBindValueMap }
        response = self._call_api('POST', 'assessments', body)
        if response is None:
            return

        assessment_id = response['id']

        time.sleep(WAIT_SECONDS)

        # wait until sql_set is ready
        while True:
            response = self._call_api('GET', 'assessments/' + assessment_id)
            if response['status'] == 1:
                self._logger.info('  processed sessions:' + str(response['jobs'][0]['count']))
                break

            if 'jobs' in response and len(response['jobs']) > 0:
                if 'count' in response['jobs'][0] and response['jobs'][0]['count'] is not None:
                    self._logger.info('  current processed sessions:' + str(response['jobs'][0]['count']) + ' ' + str(response['jobs'][0]['percent']) + '%')
                else:
                    self._logger.warning('  not started ...')
            else:
                self._logger.info('  preparing ...')
            time.sleep(WAIT_SECONDS)

        return response['id']

    def download_assessment_csv(self, assessment_id, csv_type = 'basic', download_success = False, download_failed = True):
        CHUNK_SIZE = 1024
        if not (download_success or download_failed):
            self._logger.warning('Eather download_success or download_failed must be True.')
            return None

        response = self._call_api('GET', 'assessments/' + assessment_id)
        if response is None:
            return None

        assessment_name = response['name']
        file_name = assessment_name + '.csv'
    
        self._logger.info('Donwload Assessment csv: ' + file_name)

        query_parameter = '?type=' + csv_type
        query_parameter += '&result='
        if download_success:
            query_parameter += 'success'
        if download_failed:
            if download_success:
                query_parameter += '%2C'
            query_parameter += 'failed'

        url = self._url_base + 'assessments/' + assessment_id + '/download/csv' + query_parameter
        response = requests.get(url, stream=True, cookies=self._cookies)
        if response.status_code == 200:
            with open(file_name, 'wb') as file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    file.write(chunk)

        return file_name
