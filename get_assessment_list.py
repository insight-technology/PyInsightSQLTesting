import insight_database_testing

IP_ADDRESS = '<your Insight DT Manager ip address>'
URLBASE = 'http://' + IP_ADDRESS + ':7777/idt/'
IDT_USER = '<your user name>'
IDT_PASS = '<your user password>'

if __name__ == '__main__':
    with insight_database_testing.InsightDatabaseTesting(URLBASE, IDT_USER, IDT_PASS) as idt:
        idt.print_assessments()
