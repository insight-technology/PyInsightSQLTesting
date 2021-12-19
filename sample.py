import insight_database_testing

IP_ADDRESS = '<your Insight DT Manager ip address>'
URLBASE = 'http://' + IP_ADDRESS + ':7777/idt/'
ADMIN_USER = 'administrator'
ADMIN_PASS = '<your administrator password>'

TEST_DB_NAME = '<Target DB name>'
TEST_DB_USER = '<database user>'
TEST_DB_PASS = '<database user password>'

TEST_SQL_WORKLOAD_NAME = '<SQL-workload name>'
TEST_DB_TYPE = 'PG'
TEST_DB_CONNECTION_STRING = '<test DB connection string>'
TEST_DB_VERSION = '<test DB version>'

TEST_ASSESSMENT_NAME = '<Assessment name>'

TEST_SQL_TEXT = 'select * from emp where empno = $1'
TEST_SQL_BIND = { 'name': 'EMPNO', 'value': '7369', 'type': 'INTEGER'}
TEST_CSV_FILE_NAME = '<test CSV filename>'

if __name__ == '__main__':
    test_user_pass = 'test_user'
    user_id = None

    with insight_database_testing.InsightDatabaseTesting(URLBASE, ADMIN_USER, ADMIN_PASS) as idt:
        print(idt.get_version())
        print(idt.get_license()['data'])

        # Test User operation
        print(idt.list_users())
        user_info = idt.create_user(test_user_pass, test_user_pass)
        user_id = user_info['id']
        print(idt.list_users())
        print(idt.get_user(user_id))

    with insight_database_testing.InsightDatabaseTesting(URLBASE, test_user_pass, test_user_pass) as idt:
        print(idt.get_version())

        # Test Database operation
        print('-- create-db --')
        db_info = idt.create_database(TEST_DB_NAME, TEST_DB_TYPE, TEST_DB_VERSION, TEST_DB_CONNECTION_STRING)
        print(db_info)
        db_id = db_info['id']
        print(idt.list_databases())
        print(idt.get_database(db_id))
        print(idt.test_connect_database(TEST_DB_USER, TEST_DB_PASS, TEST_DB_TYPE, TEST_DB_CONNECTION_STRING))
        print(idt.try_parse_sql(TEST_DB_USER, TEST_DB_PASS, TEST_DB_TYPE, TEST_DB_CONNECTION_STRING, TEST_SQL_TEXT))
        print(idt.try_execute_sql(TEST_DB_USER, TEST_DB_PASS, TEST_DB_TYPE, TEST_DB_CONNECTION_STRING, TEST_SQL_TEXT, False, False, [TEST_SQL_BIND]))
        print(idt.get_query_plan(TEST_DB_USER, TEST_DB_PASS, TEST_DB_TYPE, TEST_DB_CONNECTION_STRING, TEST_SQL_TEXT, False, [TEST_SQL_BIND]))
    
        # SQL-workload operation
        print('-- create sql workload --')
        sql_workload_info = idt.create_sql_workload_upload(TEST_SQL_WORKLOAD_NAME, TEST_DB_TYPE, TEST_CSV_FILE_NAME)
        print(sql_workload_info)
        sql_workload_id = sql_workload_info['id']
        idt.print_sql_workloads()

        # Assessment operation
        print('-- execute assessment')
        assessment_info = idt.execute_assessment(TEST_ASSESSMENT_NAME, sql_workload_id, [TEST_DB_USER], [TEST_DB_PASS], db_id)
        idt.print_assessments()

        print('-- delete assessment')
        idt.delete_assessment(assessment_info['id'])

        # SQL-workload operation
        print('-- delete sql workload --')
        idt.delete_sql_workload(sql_workload_id)
        idt.print_sql_workloads()

        # Test Database operation
        print('-- delete database --')
        print(idt.delete_database(db_id))
        print(idt.list_databases())

    with insight_database_testing.InsightDatabaseTesting(URLBASE, ADMIN_USER, ADMIN_PASS) as idt:
        # Test User operation
        idt.delete_user(user_id)
        print(idt.list_users())
