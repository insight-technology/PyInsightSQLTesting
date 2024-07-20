import insight_sql_testing

IP_ADDRESS = '<your Insight SQL Testing Manager ip address>'
URLBASE = 'http://' + IP_ADDRESS + ':7777/idt/'

SQL_TESTING_USER = '<SQL Testing user>'
SQL_TESTING_PASS = '<SQL Testing user password>'

SQL_WORKLOAD_NAME = '<SQL-workload name>'
SQL_WORKLOAD_CSV_FILE_NAME = 'sample.csv'
SQL_WORKLOAD_DB_TYPE = 'MYSQL'

TARGET_DB_NAME = '<Target DB name>'
CMP_SOURCE_DB_NAME = '<Cmp Target DB name>'

TEST_ASSESSMENT_NAME = '<Assessment name>'
TARGET_DB_USER = '<Target DB user>'
TARGET_DB_PASS = '<Target DB user password>'


if __name__ == '__main__':
    with insight_sql_testing.InsightSQLTesting(URLBASE, SQL_TESTING_USER, SQL_TESTING_PASS) as sql_testing:
        print(sql_testing.get_version())
  
        # SQL-workload operation
        print('-- create sql workload --')
        sql_workload_info = sql_testing.create_sql_workload_upload(SQL_WORKLOAD_NAME, SQL_WORKLOAD_DB_TYPE, SQL_WORKLOAD_CSV_FILE_NAME)
        print(sql_workload_info)
        sql_workload_id = sql_workload_info['id']
        #sql_testing.print_sql_workloads()

        # Assessment operation
        print('-- execute assessment')
        target_db_id = sql_testing.get_database_id_from_name(TARGET_DB_NAME)
        cmp_source_db_id = sql_testing.get_database_id_from_name(CMP_SOURCE_DB_NAME)
        assessment_info = sql_testing.execute_assessment(
            TEST_ASSESSMENT_NAME,
            sql_workload_id,
            [TARGET_DB_USER], [TARGET_DB_PASS],
            target_db_id = target_db_id, cmp_source_db_id = cmp_source_db_id,
            memo = 'sample assessment')
        print(assessment_info)
        #sql_testing.print_assessments()
