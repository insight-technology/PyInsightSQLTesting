import insight_database_testing
from logging import getLogger, StreamHandler, Formatter, INFO

# FORMAT_STRING = '%(asctime)s %(name)s:%(lineno)s %(funcName)s [%(levelname)s]: %(message)s'
FORMAT_STRING = '%(asctime)s [%(levelname)s]: %(message)s'
# Logger setting
LOG_LEVEL = INFO

IP_ADDRESS = '<your Insight DT Manager ip address>'
URLBASE = 'http://' + IP_ADDRESS + ':7777/idt/'
IDT_USER = '<your user name>'
IDT_PASS = '<your user password>'

if __name__ == '__main__':
    with insight_database_testing.InsightDatabaseTesting(URLBASE, IDT_USER, IDT_PASS) as idt:
        idt.print_assessments()
