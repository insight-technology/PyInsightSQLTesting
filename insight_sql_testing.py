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

class InsightSQLTesting():
    def __init__(self, url_base, user, password, upper_logger = None):
        """
        Create Insight SQL Testing session.

        Parameters
        ----------
        url_base : string
            URL to Insight DT Manager
            example: http://127.0.0.1:7777/idt/
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
        elif method == 'PATCH':
            r = requests.patch(url, headers=HEADERS, json=body, cookies=self._cookies)
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

    def _list_elements_part(self, list_key, limit = PAGE_LIMIT, offset = 0, query_parameters = None):
        query_parameter_string = ''
        if query_parameters is not None:
            for k,v in query_parameters.items():
                query_parameter_string += '&' + k + '=' + v
        elements = []
        response = self._call_api('GET', list_key + '?limit=' + str(limit) + '&offset=' + str(offset) + query_parameter_string)
        if response is None:
            return None

        # search id from name
        for target_element in response['rows']:
            elements.append(target_element.copy())

        return elements

    def _list_elements(self, list_key, query_parameters = None):
        elements = []
        for offset in range(0, MAX_ELEMENTS, PAGE_LIMIT):
            elemens_part = self._list_elements_part(list_key, PAGE_LIMIT, offset, query_parameters)
            if elemens_part is None:
                break
            if len(elemens_part) == 0:
                break

            elements.extend(elemens_part)

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
    
    def _set_optional_parameter(self, body, key, val):
        if val is not None:
            body[key] = val
    
    def _wait_until_ready(self, key, id, progress_comment, progress_key):
        time.sleep(WAIT_SECONDS)
        # wait until statusEx becomes 0(ready)
        while True:
            response = self._call_api('GET', key + '/' + id)
            if response['statusEx'] == 0:
                # finished
                self._logger.info('  ' + progress_comment + ':' + str(response['jobs'][0][progress_key]))
                break

            if 'jobs' in response and len(response['jobs']) > 0:
                if progress_key in response['jobs'][0] and response['jobs'][0][progress_key] is not None:
                    self._logger.info('  current ' + progress_comment + ':' + str(response['jobs'][0][progress_key]))
                else:
                    self._logger.warning('  not started ...')
            else:
                self._logger.info('  preparing ...')
            time.sleep(WAIT_SECONDS)
        return response
    
    def _download_file(self, url, file_name):
        CHUNK_SIZE = 1024
        response = requests.get(url, stream=True, cookies=self._cookies)
        if response.status_code == 200:
            with open(file_name, 'wb') as file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    file.write(chunk)
        else:
            return None

        return file_name

    # for Version information

    def get_version(self):
        self._logger.info('Get Version')
        version_info = self._call_api('GET', 'version')
        if version_info['VERSION'][0] != '3':
            self._logger.warn('This PyInsightSQLTesting is tested on Insight SQL Testing Version 3.x. It may does not work properly for this version.')
        return version_info

    # for License operation (admin)

    def get_license(self):
        self._logger.info('Get License')
        return self._call_api('GET', 'license')

    def update_license(self, license_key):
        self._logger.info('Update License: ' + license_key)
        body = { 'key': license_key }
        return self._call_api('PUT', 'license', body)

    # for User operation (admin)

    def list_users(self):
        self._logger.info('Get user list')
        return self._call_api('GET', 'users')

    def create_user(self, user_name, password):
        self._logger.info('Create a user: ' + user_name)
        body = { 'name': user_name, 'password': password }
        return self._call_api('POST', 'users', body)

    def get_user(self, user_id):
        self._logger.info('Get the user: ' + user_id)
        body = { 'userId': user_id }
        return self._call_api('GET', 'users/' + user_id)

    def delete_user(self, user_id):
        self._logger.info('Delete the user: ' + user_id)
        body = { 'userId': user_id }
        return self._call_api('DELETE', 'users/' + user_id)

    def reset_user_password(self, user_id):
        self._logger.info('Reset the user password: ' + user_id)
        body = { 'userId': user_id }
        return self._call_api('POST', 'users/' + user_id + '/reset-password')

    # for User operation

    def get_my_user_info(self):
        self._logger.info('Get my user info')
        return self._call_api('GET', 'users/me')

    def change_my_password(self, current_password, password, password_confirmation):
        self._logger.info('Change my password')
        body = { 'currentPassword': current_password, 'password': password, 'passwordConfirmation': password_confirmation }
        return self._call_api('POST', 'users/change-my-password', body)

    # for Database operation

    def list_databases(self):
        return self._list_elements('databases')

    def get_database_id_from_name(self, database_name):
        return self._get_id_from_name('databases', database_name)

    def create_database(self, database_name, db_type, db_version, connection_string, memo = None):
        self._logger.info('Create a database: ' + database_name)
        body = { 'name': database_name, 'dbType': db_type, 'dbVersion': db_version, 'connectionString': connection_string }
        self._set_optional_parameter(body, 'memo', memo)
        return self._call_api('POST', 'databases', body)

    def get_database(self, database_id):
        self._logger.info('Get the database: ' + database_id)
        return self._call_api('GET', 'databases/' + database_id)

    def update_database(self, database_id, database_name = None, db_type = None, db_version = None, connection_string = None, memo = None):
        self._logger.info('Update the database: ' + database_id)
        body = {}
        self._set_optional_parameter(body, 'name', database_name)
        self._set_optional_parameter(body, 'dbType', db_type)
        self._set_optional_parameter(body, 'dbVersion', db_version)
        self._set_optional_parameter(body, 'connectionString', connection_string)
        self._set_optional_parameter(body, 'memo', memo)
        if len(body) == 0:
            # do nothing
            return
        return self._call_api('PATCH', 'databases/' + database_id, body)

    def delete_database(self, database_id):
        self._logger.info('Delete the database: ' + database_id)
        return self._call_api('DELETE', 'databases/' + database_id)

    def test_connect_database(self, database_user, database_password, db_type, connection_string):
        self._logger.info('Test connect to the database: ' + database_user + ', ' + connection_string)
        body = { 'user': database_user, 'pass': database_password, 'dbType': db_type, 'connectionString': connection_string }
        return self._call_api('POST', 'databases/test-connect', body)

    def try_parse_sql(self, database_user, database_password, db_type, connection_string, sql_text, convert_parameter = False, query_timeout_millisec = None):
        self._logger.info('Try parse a SQL: ' + database_user + ', ' + connection_string + ', ' + sql_text)
        body = { 'user': database_user, 'pass': database_password, 'dbType': db_type, 'connectionString': connection_string, 'sqlText': sql_text, 'convertParameter': convert_parameter, 'queryTimeoutMillisec': query_timeout_millisec }
        return self._call_api('POST', 'databases/parse', body)

    def try_execute_sql(self, database_user, database_password, db_type, connection_string, sql_text, persist = False, convert_parameter = False, bind = [], query_timeout_millisec = None):
        self._logger.info('Try execute a SQL: ' + database_user + ', ' + connection_string + ', ' + sql_text)
        body = { 'user': database_user, 'pass': database_password, 'dbType': db_type, 'connectionString': connection_string, 'sqlText': sql_text, 'persist': persist, 'convertParameter': convert_parameter, 'bind': bind, 'queryTimeoutMillisec': query_timeout_millisec }
        return self._call_api('POST', 'databases/execute', body)

    def get_query_plan(self, database_user, database_password, db_type, connection_string, sql_text, convert_parameter = False, bind = []):
        self._logger.info('Get a SQL query plan: ' + database_user + ', ' + connection_string + ', ' + sql_text)
        body = { 'user': database_user, 'pass': database_password, 'dbType': db_type, 'connectionString': connection_string, 'sqlText': sql_text, 'convertParameter': convert_parameter, 'bind': bind }
        return self._call_api('POST', 'databases/query-plan', body)

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

    def create_sql_workload(self, sql_workload_name, db_type, source_file_name, is_unique = False, memo = None):
        self._logger.info('Create a SQL-workload: ' + sql_workload_name)
        body = { 'name': sql_workload_name, 'dbType': db_type, 'dataKind': 'MS', 'source': source_file_name, 'unique': ('true' if is_unique else 'false') }
        self._set_optional_parameter(body, 'memo', memo)
        response = self._call_api('POST', 'sql-workloads/', body)
        if response is None:
            return None

        sql_workload_id = response['id']
        return self._wait_until_ready('sql-workloads', sql_workload_id, 'processed sqls', 'count')

    def create_sql_workload_upload(self, sql_workload_name, db_type, source_file_path, is_unique = False, memo = None):
        self._logger.info('Create a SQL-workload(upload): ' + sql_workload_name)
        file_content = open(source_file_path, 'rb')
        files = {'source': ('upload_file', file_content, 'text/csv')}
        body = { 'name': sql_workload_name, 'dbType': db_type, 'dataKind': 'MS', 'unique': ('true' if is_unique else 'false') }
        self._set_optional_parameter(body, 'memo', memo)
        response = self._call_api('POST_UPLOAD', 'sql-workloads/upload', body, files)
        if response is None:
            return None

        sql_workload_id = response['id']
        return self._wait_until_ready('sql-workloads', sql_workload_id, 'processed sqls', 'count')
    
    def get_sql_workload(self, sql_workload_id):
        self._logger.info('Get tje SQL-workload: ' + sql_workload_id)
        return self._call_api('GET', 'sql-workloads/' + sql_workload_id)

    def get_sql_workload_id_from_name(self, sql_workload_name):
        return self._get_id_from_name('sql-workloads', sql_workload_name)

    def update_sql_workload(self, sql_workload_id, sql_workload_name = None, db_type = None, memo = None):
        self._logger.info('Update the SQL-workload: ' + sql_workload_id)
        body = {}
        self._set_optional_parameter(body, 'name', sql_workload_name)
        self._set_optional_parameter(body, 'dbType', db_type)
        self._set_optional_parameter(body, 'memo', memo)
        if len(body) == 0:
            # do nothing
            return
        return self._call_api('PATCH', 'sql-workloads/' + sql_workload_id, body)

    def delete_sql_workload(self, sql_workload_id):
        self._logger.info('Delete the SQL-workload: ' + sql_workload_id)
        return self._call_api('DELETE', 'sql-workloads/' + sql_workload_id)

    # Deprecated
    def get_sql_workload_summary(self, sql_workload_id):
        self._logger.info('Get SQL-workload summary: ' + sql_workload_id)
        self._logger.warn('This API is deprecated. The information is included in SQL-workload info.')
        return self._call_api('GET', 'sql-workloads/' + sql_workload_id + '/summary')

    def get_sql_workload_sqls(self, sql_workload_id, limit = PAGE_LIMIT, offset = 0, query_parameters = None):
        self._logger.info('Get SQL-workload SQLs: ' + sql_workload_id + ' (limit=' + str(limit) + ', offset=' + str(offset) + ')')
        return self._list_elements_part('sql-workloads/' + sql_workload_id + '/rows', limit, offset, query_parameters)

    def get_sql_workload_sqls_all(self, sql_workload_id):
        self._logger.info('Get SQL-workload all SQLs (This operation may take long time to be processed.): ' + sql_workload_id)
        return self._list_elements('sql-workloads/' + sql_workload_id + '/rows')

    def copy_sql_workload(self, sql_workload_id, name):
        self._logger.info('Copy the SQL-workload: ' + sql_workload_id + ' (name=' + name + ')')
        body = { 'name': name }
        return self._call_api('POST', 'sql-workloads/' + sql_workload_id + '/copy', body)

    def update_sql_workload_db_user(self, sql_workload_id, old_users, new_users):
        self._logger.info('Update the SQL-workload DB users: ' + sql_workload_id)
        body = { 'oldusers': old_users, 'newusers': new_users }
        return self._call_api('PUT', 'sql-workloads/' + sql_workload_id + '/modify', body)

    def update_sql_workload_sqls(self, sql_workload_id):
        self._logger.info('Update the SQL-workload SQLs from SCT file: ' + sql_workload_id)
        self._logger.warn('Not supported yet.')
        # TODO: Not supported yet.
        return None

    # for Patch SQL set operation

    def list_patch_sqls(self):
        return self._list_elements('patch-sqls')

    def create_patch_sql_from_assessment(self, patch_sql_name, assessment_id, memo = None):
        self._logger.info('Create a patch sql (from assessment): ' + patch_sql_name)
        body = { 'name': patch_sql_name, 'assessmentId': assessment_id }
        self._set_optional_parameter(body, 'memo', memo)
        response = self._call_api('POST', 'patch-sqls/from-assessment', body)
        if response is None:
            return None

        patch_sql_id = response['id']
        return self._wait_until_ready('patch-sqls', patch_sql_id, 'processed(%)', 'percent')

    def create_patch_sql_upload(self, patch_sql_name, source_file_path, memo = None):
        self._logger.info('Create a patch sql (upload): ' + patch_sql_name)
        file_content = open(source_file_path, 'rb')
        files = {'source': ('upload_file', file_content, 'text/csv')}
        body = { 'name': patch_sql_name }
        self._set_optional_parameter(body, 'memo', memo)
        response = self._call_api('POST_UPLOAD', 'patch-sqls/from-sct', body, files)
        if response is None:
            return None

        patch_sql_id = response['id']
        return self._wait_until_ready('patch-sqls', patch_sql_id, 'processed(%)', 'percent')

    def merge_patch_sqls(self, patch_sql_name, patch_sqls, memo = None):
        self._logger.info('Create a patch sql (from patch sqls): ' + patch_sql_name)
        body = { 'name': patch_sql_name, 'patchSqlIds': patch_sqls }
        self._set_optional_parameter(body, 'memo', memo)
        response = self._call_api('POST', 'patch-sqls/merge', body)
        if response is None:
            return None

        patch_sql_id = response['id']
        return self._wait_until_ready('patch-sqls', patch_sql_id, 'processed(%)', 'percent')

    def get_patch_sql(self, patch_sql_id):
        self._logger.info('Get the patch sql: ' + patch_sql_id)
        return self._call_api('GET', 'patch-sqls/' + patch_sql_id)

    def get_patch_sql_id_from_name(self, patch_sql_name):
        return self._get_id_from_name('patch-sqls', patch_sql_name)

    def update_patch_sql(self, patch_sql_id, patch_sql_name = None, memo = None):
        self._logger.info('Update the patch sql: ' + patch_sql_id)
        body = {}
        self._set_optional_parameter(body, 'name', patch_sql_name)
        self._set_optional_parameter(body, 'memo', memo)
        if len(body) == 0:
            # do nothing
            return
        return self._call_api('PATCH', 'patch-sqls/' + patch_sql_id, body)

    def delete_patch_sql(self, patch_sql_id):
        self._logger.info('Delete the patch sql: ' + patch_sql_id)
        return self._call_api('DELETE', 'patch-sqls/' + patch_sql_id)

    def get_patch_sql_sqls(self, patch_sql_id, limit = PAGE_LIMIT, offset = 0, query_parameters = None):
        self._logger.info('Get patch sql SQLs: ' + patch_sql_id + ' (limit=' + str(limit) + ', offset=' + str(offset) + ')')
        return self._list_elements_part('patch-sqls/' + patch_sql_id + '/hash-rule/rows', limit, offset, query_parameters)

    def get_patch_sql_sqls_all(self, patch_sql_id):
        self._logger.info('Get patch sql SQLs (This operation may take long time to be processed.): ' + patch_sql_id)
        return self._list_elements('patch-sqls/' + patch_sql_id + '/hash-rule/rows')

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

                        all_codes = assessment['summary']['allCode']
                        if assessment['cmpDatabaseId'] is not None:
                            # 2DB
                            print('    Assessment summary:')
                            print('           Tgt-DB Failed:'+str(all_codes[1]))
                            print('             Both Failed:'+str(all_codes[3]))
                            print('       Different returns:'+str(all_codes[4]))
                            print(' Performance degradation:'+str(all_codes[5]))
                            print('                 Success:'+str(all_codes[0]))
                            print('      Test src-DB Failed:'+str(all_codes[2]))
                        else:
                            # 1DB
                            print('    Assessment summary: Success:'+str(all_codes[0])+', Failed:'+str(all_codes[1]))
                    else:
                        print('    not finished.')
                else:
                    print('    jobs element is empty.')
            else:
                print('    no jobs element.')

    def execute_assessment(self, assessment_name, sql_workload_id,
        db_users, db_user_passwords,
        target_db_id, cmp_source_db_id = None,
        patch_sql_workload_id = None, cmp_patch_sql_workload_id = None,
        memo = None,
        start = '00000000000000', end = '99999999999999',
        is_serial_mode = False, sql_start_time_begin = None, sql_start_time_end = None,
        exec_level = 'E', transaction = 'N',
        result_record = None, query_plan_record = None, compare_on_disk = None,
        alt_users = None, cmp_alt_users = None,
        etime_zero = 0.2, concurrency = 1, convert_parameter = False,
        filling_bind_value_map = {'UNKNOWN': { 'type': 'UNKNOWN', 'value':'-'}},
        cmp_filling_bind_value_map = {'UNKNOWN': { 'type': 'UNKNOWN', 'value':'-'}},
        header_comparison_level = 'STRICT', order_comparison_level = 'STRICT',
        query_timeout_millisec = None,
        time_threshold = None, ratio_threshold = None,
        trim_char = False, epsilon = None,
        fetch_size = None, fetch_limit = None,
        hook = None, cmp_hook = None, ses_hook = None, cmp_ses_hook = None,
        cmp_pswds = None):
        self._logger.info('Execute an assessment: ' + assessment_name)
        body = {
            'name': assessment_name,
            'memo': memo,
            'sqlWorkloadId': sql_workload_id,
            'databaseId': target_db_id, 'cmpDatabaseId': cmp_source_db_id,
            'patchSqlId': patch_sql_workload_id, 'cmpPatchSqlId': cmp_patch_sql_workload_id,
            'start': start, 'end': end,
            'isSerialMode': is_serial_mode, 'sqlStartTimeBegin': sql_start_time_begin, 'sqlStartTimeEnd': sql_start_time_end,
            'execLevel': exec_level, 'transaction': transaction,
            'resultRecord': result_record, 'queryPlanRecord': query_plan_record, 'compareOnDisk': compare_on_disk,
            'users': db_users, 'altUsers': alt_users, 'cmpAltUsers': cmp_alt_users,
            'etimeZero': etime_zero, 'concurrency': concurrency, 'convertParameter': convert_parameter,
            'fillingBindValueMap': filling_bind_value_map, 'cmpFillingBindValueMap': cmp_filling_bind_value_map,
            'headerComparisonLevel': header_comparison_level, 'orderComparisonLevel': order_comparison_level,
            'queryTimeoutMillisec': query_timeout_millisec,
            'timeThreshold': time_threshold, 'ratioThreshold': ratio_threshold,
            'trimChar': trim_char, 'epsilon': epsilon,
            'fetchSize': fetch_size, 'fetchLimit': fetch_limit,
            'hook': hook, 'cmp': cmp_hook, 'sesHook': ses_hook, 'cmpSesHook': cmp_ses_hook,
            'pswds': db_user_passwords, 'cmpPswds': cmp_pswds
        }
        response = self._call_api('POST', 'assessments', body)
        if response is None:
            return None

        assessment_id = response['id']
        return self._wait_until_ready('assessments', assessment_id, 'processed sessions', 'count')

    def get_assessment(self, assessment_id):
        self._logger.info('Get the assessment: ' + assessment_id)
        return self._call_api('GET', 'assessments/' + assessment_id)

    def get_assessment_id_from_name(self, assessment_name):
        return self._get_id_from_name('assessments', assessment_name)

    def update_assessment(self, assessment_id, assessment_name = None, memo = None):
        self._logger.info('Update the assessment: ' + assessment_id)
        body = {}
        self._set_optional_parameter(body, 'name', assessment_name)
        self._set_optional_parameter(body, 'memo', memo)
        if len(body) == 0:
            # do nothing
            return
        return self._call_api('PATCH', 'assessments/' + assessment_id, body)

    def delete_assessment(self, assessment_id):
        self._logger.info('Delete the assessment: ' + assessment_id)
        return self._call_api('DELETE', 'assessments/' + assessment_id)

    def get_assessment_sqls(self, assessment_id, limit = PAGE_LIMIT, offset = 0, query_parameters = None):
        self._logger.info('Get assessment SQLs: ' + assessment_id + ' (limit=' + str(limit) + ', offset=' + str(offset) + ')')
        return self._list_elements_part('assessments/' + assessment_id + '/results', limit, offset, query_parameters)

    def get_assessment_sqls_all(self, assessment_id, query_parameters = None):
        self._logger.info('Get assessment SQLs (This operation may take long time to be processed.): ' + assessment_id)
        return self._list_elements('assessments/' + assessment_id + '/results', query_parameters)

    def get_assessment_sql(self, assessment_id, assessment_row_id):
        self._logger.info('Get assessment SQL: ' + assessment_id + ' (assessment_row_id=' + str(assessment_row_id) + ')')
        return self._call_api('GET', 'assessments/' + assessment_id + '/results/' + str(assessment_row_id))

    def get_assessment_sql_query_rows(self, assessment_id, assessment_row_id, offset = 0):
        self._logger.info('Get assessment SQL query rows: ' + assessment_id + ' (assessment_row_id=' + str(assessment_row_id) + ')')
        return self._call_api('GET', 'assessments/' + assessment_id + '/results/' + str(assessment_row_id) + '/queryRows?offset=' + str(offset))

    def get_assessment_sql_query_rows_all(self, assessment_id, assessment_row_id):
        self._logger.info('Get assessment SQL query rows (This operation may take long time to be processed.): ' + assessment_id + ' (assessment_row_id=' + str(assessment_row_id) + ')')
        elements = []
        for offset in range(0, MAX_ELEMENTS, 100):
            elemens_part = self.get_assessment_sql_query_rows(assessment_id, assessment_row_id, offset)
            if elemens_part is None:
                break
            if len(elemens_part) == 0:
                break

            elements.extend(elemens_part)
        return elements
    
    def download_assessment_sql_query_rows(self, assessment_id, assessment_row_id):
        CHUNK_SIZE = 1024
        response = self._call_api('GET', 'assessments/' + assessment_id)
        if response is None:
            return None

        assessment_name = response['name']
        file_name = assessment_name + '_' + str(assessment_row_id) + '_returns(tgt).csv'
    
        self._logger.info('Donwload the assessment SQL query rows(csv): ' + file_name)

        url = self._url_base + 'assessments/' + assessment_id + '/results/' + str(assessment_row_id) + '/queryRows/download?format=csv'
        return self._download_file(url, file_name)

    def get_assessment_sql_cmp_query_rows(self, assessment_id, assessment_row_id, offset = 0):
        self._logger.info('Get assessment SQL query rows(cmp): ' + assessment_id + ' (assessment_row_id=' + str(assessment_row_id) + ')')
        return self._call_api('GET', 'assessments/' + assessment_id + '/results/' + str(assessment_row_id) + '/cmpQueryRows?offset=' + str(offset))

    def get_assessment_sql_cmp_query_rows_all(self, assessment_id, assessment_row_id):
        self._logger.info('Get assessment SQL query rows(cmp) (This operation may take long time to be processed.): ' + assessment_id + ' (assessment_row_id=' + str(assessment_row_id) + ')')
        elements = []
        for offset in range(0, MAX_ELEMENTS, 100):
            elemens_part = self.get_assessment_sql_cmp_query_rows(assessment_id, assessment_row_id, offset)
            if elemens_part is None:
                break
            if len(elemens_part) == 0:
                break

            elements.extend(elemens_part)
        return elements

    def download_assessment_sql_cmp_query_rows(self, assessment_id, assessment_row_id):
        CHUNK_SIZE = 1024
        response = self._call_api('GET', 'assessments/' + assessment_id)
        if response is None:
            return None

        assessment_name = response['name']
        file_name = assessment_name + '_' + str(assessment_row_id) + '_returns(cmp).csv'
    
        self._logger.info('Donwload the assessment SQL query rows(cmp, csv): ' + file_name)

        url = self._url_base + 'assessments/' + assessment_id + '/results/' + str(assessment_row_id) + '/cmpQueryRows/download?format=csv'
        return self._download_file(url, file_name)

    def download_assessment_csv(self, assessment_id, csv_type = 'basic', result_code = '1,2,3,4,5'):
        CHUNK_SIZE = 1024
        if result_code is None or result_code == '':
            self._logger.warning('result_code must not be empty.')
            return None

        response = self._call_api('GET', 'assessments/' + assessment_id)
        if response is None:
            return None

        assessment_name = response['name']
        file_name = assessment_name + '.csv'
    
        self._logger.info('Donwload the assessment csv: ' + file_name)

        query_parameter = '?type=' + csv_type
        query_parameter += '&resultCode=' + result_code

        url = self._url_base + 'assessments/' + assessment_id + '/download/csv' + query_parameter
        return self._download_file(url, file_name)
