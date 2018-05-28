# encoding: utf-8

import os

from airflow import settings
#from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import CONN_ENV_PREFIX

CONN_ID = "dag_creation_manager_plugin_sql_alchemy_conn"


# def get_mysql_hook():
#     os.environ[CONN_ENV_PREFIX + CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
#     return MySqlHook(mysql_conn_id=CONN_ID)


def get_postgresql_hook():
    os.environ[CONN_ENV_PREFIX + CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
    return PostgresHook(postgres_conn_id=CONN_ID)


def run_sql(sql, ignore_error=False):
    hook = get_postgresql_hook()

    print("sql:\n%s" % sql)
    try:
        res = hook.run(sql)
    except Exception as e:
        #if not ignore_error:
        raise e
        res = None

    print(res)
    return res


def run_version_0_0_1():
    run_sql("""
      CREATE SEQUENCE IF NOT EXISTS dcmp_dag_seq;

      CREATE TABLE IF NOT EXISTS dcmp_dag (
              id int NOT NULL DEFAULT NEXTVAL ('dcmp_dag_seq'),
              dag_name varchar(250) NOT NULL,
              version int NOT NULL,
              category varchar(50) NOT NULL,
              editing boolean NOT NULL,
              editing_user_id int DEFAULT NULL,
              editing_user_name varchar(250) DEFAULT NULL,
              last_editor_user_id int DEFAULT NULL,
              last_editor_user_name varchar(250) DEFAULT NULL,
              updated_at timestamp(6) NOT NULL,
              PRIMARY KEY (id),
              CONSTRAINT dag_name UNIQUE  (dag_name)
            );
      CREATE INDEX IF NOT EXISTS category ON dcmp_dag (category);
      CREATE INDEX IF NOT EXISTS editing ON dcmp_dag (editing);
      CREATE INDEX IF NOT EXISTS updated_at ON dcmp_dag (updated_at);            
    """)


    run_sql("""
    CREATE SEQUENCE IF NOT EXISTS dcmp_dag_conf_seq;

    CREATE TABLE IF NOT EXISTS dcmp_dag_conf (
              id int NOT NULL DEFAULT NEXTVAL ('dcmp_dag_conf_seq'),
              dag_id int NOT NULL,
              dag_name varchar(250) NOT NULL,
              action varchar(50) NOT NULL,
              version int NOT NULL,
              conf text NOT NULL,
              creator_user_id int DEFAULT NULL,
              creator_user_name varchar(250) DEFAULT NULL,
              created_at timestamp(6) NOT NULL,
              PRIMARY KEY (id)
            );
    
    CREATE INDEX IF NOT EXISTS dag_id ON dcmp_dag_conf (dag_id);
    CREATE INDEX IF NOT EXISTS dag_name ON dcmp_dag_conf (dag_name);
    CREATE INDEX IF NOT EXISTS action ON dcmp_dag_conf (action);
    CREATE INDEX IF NOT EXISTS version ON dcmp_dag_conf (version);
    CREATE INDEX IF NOT EXISTS created_at ON dcmp_dag_conf (created_at);
    """)


def run_version_0_0_2():
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS editing_start timestamp(6);",
            ignore_error=True)
    run_sql("CREATE INDEX IF NOT EXISTS editing_start ON dcmp_dag (editing_start);",
            ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS last_edited_at timestamp(6);",
            ignore_error=True)
    run_sql("CREATE INDEX IF NOT EXISTS last_edited_at ON dcmp_dag (last_edited_at);",
            ignore_error=True)

def run_version_0_1_1():
    pass
    #run_sql("ALTER TABLE dcmp_dag_conf ALTER conf TYPE text;")


def run_version_0_2_0():
    run_sql("""
        CREATE SEQUENCE IF NOT EXISTS dcmp_user_profile_seq;

        CREATE TABLE IF NOT EXISTS dcmp_user_profile (
                  id int NOT NULL DEFAULT NEXTVAL ('dcmp_user_profile_seq'),
                  user_id int NOT NULL,
                  is_superuser boolean NOT NULL,
                  is_data_profiler boolean NOT NULL,
                  is_approver boolean NOT NULL,
                  updated_at timestamp(6) NOT NULL,
                  created_at timestamp(6) NOT NULL,
                  PRIMARY KEY (id)
                );
        
        CREATE INDEX IF NOT EXISTS user_id ON dcmp_user_profile (user_id);
        CREATE INDEX IF NOT EXISTS is_superuser ON dcmp_user_profile (is_superuser);
        CREATE INDEX IF NOT EXISTS is_data_profiler ON dcmp_user_profile (is_data_profiler);
        CREATE INDEX IF NOT EXISTS is_approver ON dcmp_user_profile (is_approver);
        CREATE INDEX IF NOT EXISTS updated_at ON dcmp_user_profile (updated_at);
        CREATE INDEX IF NOT EXISTS created_at ON dcmp_user_profile (created_at);
    """)


def run_version_0_2_1():
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS approved_version int NOT NULL;", ignore_error=True)
    run_sql("CREATE INDEX IF NOT EXISTS approved_version ON dcmp_dag (approved_version);", ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS approver_user_id int DEFAULT NULL;", ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS approver_user_name varchar(250) DEFAULT NULL;", ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag ADD COLUMN IF NOT EXISTS last_approved_at timestamp(6);", ignore_error=True)
    run_sql("CREATE INDEX IF NOT EXISTS last_approved_at ON dcmp_dag (last_approved_at);", ignore_error=True)

    run_sql("ALTER TABLE dcmp_dag_conf ADD COLUMN IF NOT EXISTS approver_user_id int DEFAULT NULL;", ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag_conf ADD COLUMN IF NOT EXISTS approver_user_name varchar(250) DEFAULT NULL;", ignore_error=True)
    run_sql("ALTER TABLE dcmp_dag_conf ADD COLUMN IF NOT EXISTS approved_at timestamp(6);", ignore_error=True)
    run_sql("CREATE INDEX IF NOT EXISTS approved_at ON dcmp_dag_conf (approved_at);", ignore_error=True)

    run_sql("ALTER TABLE dcmp_user_profile ADD COLUMN IF NOT EXISTS approval_notification_emails text NOT NULL;", ignore_error=True)


### Below migrations for MySQL
# def run_version_0_0_1():
#     run_sql("""
#         CREATE TABLE IF NOT EXISTS `dcmp_dag` (
#           `id` int(11) NOT NULL AUTO_INCREMENT,
#           `dag_name` varchar(250) NOT NULL,
#           `version` int(11) NOT NULL,
#           `category` varchar(50) NOT NULL,
#           `editing` tinyint(1) NOT NULL,
#           `editing_user_id` int(11) DEFAULT NULL,
#           `editing_user_name` varchar(250) DEFAULT NULL,
#           `last_editor_user_id` int(11) DEFAULT NULL,
#           `last_editor_user_name` varchar(250) DEFAULT NULL,
#           `updated_at` datetime(6) NOT NULL,
#           PRIMARY KEY (`id`),
#           UNIQUE KEY `dag_name` (`dag_name`),
#           KEY `category` (`category`),
#           KEY `editing` (`editing`),
#           KEY `updated_at` (`updated_at`)
#         ) DEFAULT CHARSET=utf8mb4;
#     """)
#
#     run_sql("""
#         CREATE TABLE IF NOT EXISTS `dcmp_dag_conf` (
#           `id` int(11) NOT NULL AUTO_INCREMENT,
#           `dag_id` int(11) NOT NULL,
#           `dag_name` varchar(250) NOT NULL,
#           `action` varchar(50) NOT NULL,
#           `version` int(11) NOT NULL,
#           `conf` text NOT NULL,
#           `creator_user_id` int(11) DEFAULT NULL,
#           `creator_user_name` varchar(250) DEFAULT NULL,
#           `created_at` datetime(6) NOT NULL,
#           PRIMARY KEY (`id`),
#           KEY `dag_id` (`dag_id`),
#           KEY `dag_name` (`dag_name`),
#           KEY `action` (`action`),
#           KEY `version` (`version`),
#           KEY `created_at` (`created_at`)
#         ) DEFAULT CHARSET=utf8mb4;
#     """)
#
#
# def run_version_0_0_2():
#     run_sql("ALTER TABLE dcmp_dag ADD editing_start datetime(6);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD INDEX editing_start (editing_start);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD last_edited_at datetime(6);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD INDEX last_edited_at (last_edited_at);", ignore_error=True)
#
#
# def run_version_0_1_1():
#     run_sql("ALTER TABLE dcmp_dag_conf CHANGE conf conf mediumtext NOT NULL;")
#
#
# def run_version_0_2_0():
#     run_sql("""
#         CREATE TABLE IF NOT EXISTS `dcmp_user_profile` (
#           `id` int(11) NOT NULL AUTO_INCREMENT,
#           `user_id` int(11) NOT NULL,
#           `is_superuser` tinyint(1) NOT NULL,
#           `is_data_profiler` tinyint(1) NOT NULL,
#           `is_approver` tinyint(1) NOT NULL,
#           `updated_at` datetime(6) NOT NULL,
#           `created_at` datetime(6) NOT NULL,
#           PRIMARY KEY (`id`),
#           KEY `user_id` (`user_id`),
#           KEY `is_superuser` (`is_superuser`),
#           KEY `is_data_profiler` (`is_data_profiler`),
#           KEY `is_approver` (`is_approver`),
#           KEY `updated_at` (`updated_at`),
#           KEY `created_at` (`created_at`)
#         ) DEFAULT CHARSET=utf8mb4;
#     """)
#
#
# def run_version_0_2_1():
#     run_sql("ALTER TABLE dcmp_dag ADD approved_version int(11) NOT NULL;", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD INDEX approved_version (approved_version);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD approver_user_id int(11) DEFAULT NULL;", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD approver_user_name varchar(250) DEFAULT NULL;", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD last_approved_at datetime(6);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag ADD INDEX last_approved_at (last_approved_at);", ignore_error=True)
#
#     run_sql("ALTER TABLE dcmp_dag_conf ADD approver_user_id int(11) DEFAULT NULL;", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag_conf ADD approver_user_name varchar(250) DEFAULT NULL;", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag_conf ADD approved_at datetime(6);", ignore_error=True)
#     run_sql("ALTER TABLE dcmp_dag_conf ADD INDEX approved_at (approved_at);", ignore_error=True)
#
#     run_sql("ALTER TABLE dcmp_user_profile ADD approval_notification_emails text NOT NULL;", ignore_error=True)


def main():
    run_version_0_0_1()
    run_version_0_0_2()
    run_version_0_1_1()
    run_version_0_2_0()
    run_version_0_2_1()


if __name__ == "__main__":
    main()