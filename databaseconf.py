data_base_dict = {
    'qa': {
        'main_mysql': {
            'servertype': 'mysql',
            'host': '10.88.130.171',
            'port': 3306,
            'database': 'PersonalizedEngagement',
            'user': 'pe',
            'password': 'Meritco@123'
        }, 'sms_mysql': {
            'servertype': 'mysql',
            'host': '10.88.130.171',
            'port': 3306,
            'database': 'SMSChannel',
            'user': 'pe',
            'password': 'Meritco@123'
        },
        'main_sqlserver': {
            'servertype': 'sqlserver',
            'host': '10.88.130.50',
            'port': 1433,
            'database': 'PersonalizedEngagement',
            'user': 'meritcoacc',
            'password': 'M@ritc0'
        }, 'sms_sqlserver': {
            'servertype': 'sqlserver',
            'host': '10.88.130.50',
            'port': 1433,
            'database': 'SMSChannel',
            'user': 'meritcoacc',
            'password': 'M@ritc0'
        },
        'action_mysql': {
            'servertype': 'mysql',
            'host': '10.88.130.171',
            'port': 3306,
            'database': 'wx',
            'user': 'pe',
            'password': 'Meritco@123'
        }
    },
    'prod': {
        'main_mysql': {
            'servertype': 'mysql',
            'host': '10.88.7.157',
            'port': 3306,
            'database': 'PersonalizedEngagement',
            'user': 'pe',
            'password': 'Meritco@123'
        },
        'main_sqlserver': {
            'servertype': 'sqlserver',
            'host': '10.88.7.183',
            'port': 1433,
            'database': 'PersonalizedEngagement',
            'user': 'svcperseng',
            'password': 'S7cp@r1mg'
        },
        'sms_sqlserver': {
            'servertype': 'sqlserver',
            'host': '10.88.7.183',
            'port': 1433,
            'database': 'SMSChannel',
            'user': 'svcsmschan',
            'password': '5^c$nsc7@n'
        },
        'action_mysql': {
            'servertype': 'mysql',
            'host': '10.88.7.139',
            'port': 3306,
            'database': 'wx',
            'user': 'pe',
            'password': 'Meritco@123'
        }
    },
    'dev': {
        'main_mysql': {
            'servertype': 'mysql',
            'host': '192.168.0.162',
            'port': 3306,
            'database': 'PersonalizedEngagement',
            'user': 'pe',
            'password': 'Meritco@123'
        },
        'main_sqlserver': {
            'servertype': 'sqlserver',
            'host': '192.168.0.67',
            'port': 1433,
            'database': 'fe_plat',
            'user': 'fe',
            'password': 'fe'
        },
        'action_mysql': {
            'servertype': 'mysql',
            'host': '192.168.0.61',
            'port': 3306,
            'database': 'test',
            'user': 'root',
            'password': 'root'
        }
    }
}