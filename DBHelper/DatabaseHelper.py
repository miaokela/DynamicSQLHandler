import copy
import typing
import sys
import os
import re
from contextlib import contextmanager

from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql import compiler
from flask_sqlalchemy import SQLAlchemy as _SQLAlchemy
from loguru import logger
from flask import Flask, jsonify


# 单独使用时设置环境变量
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)

from DBHelper.sql_loader import SqlLoader


# Flask 数据库操作类
class DataBaseHelper:
    db = None  # flask sqlalchemy session对象，需要先赋值

    @classmethod
    def get_params_without_paginated(cls, params: typing.Dict):
        if not params:
            return {}
        params_cp = copy.deepcopy(params)
        if 'pageNum' in params:
            del params_cp['pageNum']
        if 'pageSize' in params:
            del params_cp['pageSize']
        return params_cp

    @classmethod
    def set_where_phrase(cls, sql, where):
        """
        生成where语句
        """
        if not where:
            return sql
        where_str = " WHERE "
        for key in where.keys():
            where_str += key + " = :" + "_where_%s" % key + " and "
        where_str = where_str[0:-5]
        sql += where_str
        return sql

    @classmethod
    def fullfilled_data(cls, data, where):
        """
        删除/更新操作，对传入的data 在where条件中的字段都新增一个 _where_${field} 字段，用于where条件的赋值
        """
        if not where:
            return data
        
        for k, v in where.items():
            if k.startswith("_where_"):
                raise Exception("where条件中不能包含 _where_ 开头的字段")
            data.update(**{
                "_where_%s" % k: v
            })

        return data

    # 需求：更新 插入 删除 不需要编写sql 传入表名、数据、条件即可
    @classmethod
    def execute_update(cls, tb_name, data, where, app=None, bind=None):
        """
        更新数据
            UPDATE可能存在的问题：where与data字段名称相同，值不相同的问题
            处理：            
            删除/更新操作，对传入的data 在where条件中的字段都新增一个 _where_${field} 字段，用于where条件的赋值，
            where条件的value通过这个方式转化：:field => :_where_${field}

            where就是where data就是data 处理时 对转化后的where 更新到data里 data.update(**where)
        :param tb_name: 表名
        :param data: 数据
        :param where: 过滤条件
        :return: 更新数量
        """
        sql = "UPDATE " + tb_name + " SET "
        for key in data.keys():
            sql += key + " = :" + key + ","
        sql = sql[0:-1]

        data = cls.fullfilled_data(data, where)
        sql = cls.set_where_phrase(sql, where)
        try:
            if app and bind:
                bind = cls.db.get_engine(app, bind=bind)
                result = cls.db.session.execute(sql, data, bind=bind)
            else:
                result = cls.db.session.execute(sql, data)
            return result.rowcount
        except Exception as e:
            logger.error("执行sql: < %s %s > 失败！ 原因: %s" % (sql, str(data), str(e)))
            return None

    @classmethod
    def allow_sharp(cls):
        """
        允许使用#号出现在字段名称中
        """
        compiler.BIND_PARAMS = re.compile(r"(?<![:\w\$\x5c]):([\w\$\#]+)(?![:\w\$])", re.UNICODE)
        TextClause._bind_params_regex = re.compile(r'(?<![:\\w\\x5c]):([\w\#]+)(?!:)', re.UNICODE)

    @classmethod
    def execute_create(cls, tb_name, data, app=None, bind=None):
        """
        插入数据
        :param tb_name: 表名
        :param data: 数据
        :return: 插入数据的id
        """
        cls.allow_sharp()
        sql = "INSERT INTO " + tb_name + " ("
        for key in data.keys():
            sql += "`%s`" % key + ","
        sql = sql[0:-1]
        sql += ") VALUES ("
        for key in data.keys():
            sql += ":" + key + ","
        sql = sql[0:-1]
        sql += ")"
        try:
            if app and bind:
                bind = cls.db.get_engine(app, bind=bind)
                result = cls.db.session.execute(sql, data, bind=bind)
            else:
                result = cls.db.session.execute(sql, data)
            return result.lastrowid
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error("执行sql: < %s %s > 失败！ 原因: %s" % (sql, str(data), str(e)))
            return None

    @classmethod
    def execute_delete(cls, tb_name, where, logic=False, app=None, bind=None):
        """
        删除数据
        :param tb_name: 表名
        :param where: 过滤条件
        :return: 删除数量
        """
        sql = "DELETE FROM " + tb_name
        if logic:
            sql = "UPDATE %s SET delete_flag=1" % tb_name
        sql = cls.set_where_phrase(sql, where)
        where = cls.fullfilled_data({}, where)

        try:
            if app and bind:
                bind = cls.db.get_engine(app, bind=bind)
                result = cls.db.session.execute(sql, where, bind=bind)
            else:
                result = cls.db.session.execute(sql, where)
            return result.rowcount
        except Exception as e:
            logger.error("执行sql: < %s %s > 失败！ 原因: %s" % (sql, str(where), str(e)))
            return None

    @classmethod
    def execute_sql(cls, sql_id, params=None, options: typing.Dict[str, str] = None, app=None, bind=None):
        """
        动态sql通用方法
        :param sql_id:
        :param params: 查询条件
        :param options: 动态sql条件
        :return:
        """
        s = SqlLoader()
        preloaded_sql = s.preload_sql(sql_id, options=options)
        try:
            # 支持多数据库|指定数据库执行sql
            if app and bind:
                bind = cls.db.get_engine(app, bind=bind)
                result = cls.db.session.execute(preloaded_sql, params, bind=bind).fetchall()
            else:
                print('execute <%s>, params: %s' % (sql_id, str(params)))
                result = cls.db.session.execute(preloaded_sql, params).fetchall()
        except Exception as e:
            import traceback
            traceback.print_exc()
            logger.error("执行sql: %s %s 失败！ 原因：%s" % (preloaded_sql, str(params), str(e)))
            return []
        # else:
        #     logger.info("当前执行的sql: %s %s" % (preloaded_sql, str(params)))
        return [dict(zip(item.keys(), item)) for item in result]

    @classmethod
    def select_one(cls, sql_id, params=None, options: typing.Dict[str, str] = None, app=None, bind=None):
        options = cls.get_params_without_paginated(options)  # 不需要分页
        result = cls.execute_sql(sql_id, params, options, app=app, bind=bind)
        return result[0] if result else {}

    @classmethod
    def select_all(cls, sql_id, params=None, options: typing.Dict[str, typing.Union[str, int, None]] = None, app=None, bind=None):
        return cls.execute_sql(sql_id, params, options, app=app, bind=bind)

    @classmethod
    def rollback(cls):
        cls.db.session.rollback()


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:12345678@localhost:3306/test?charset=utf8mb4'
class SQLAlchemy(_SQLAlchemy):
    """
    事务上下文管理器
    """

    @contextmanager
    def trans(self):
        try:
            yield
            self.session.commit()  # 事务
        except Exception as e:
            self.session.rollback()  # 回滚
            raise e
        
DataBaseHelper.db = SQLAlchemy(app)


@app.route('/index')
def index():
    location_name = DataBaseHelper.select_all(
        'home.index.query_sensor_location_by_id',
        params={'id': 1},
        options={'pageNum': 2, 'pageSize': 10}
    )  # !sql文件路径所在位置根据实际情况调整
    print(location_name) # !sql文件路径所在位置根据实际情况调整
    # 支持事务
    with DataBaseHelper.db.trans():  
        # 新增 execute_insert
        # 修改 execute_update
        # 删除 execute_delete
        pass

    # 或者 将db挂在app上
    # app.db = DataBaseHelper.db
    # from flask import current_app as cp 
    # with cp.db.trans(): ...
    return jsonify({'location_name': location_name})


if __name__ == '__main__':
    app.run()
    # pageNum/pageSize来控制分页
    # 查询使用的参数: params 条件 options (基于Jinjia2模板语法)动态控制sql语句部分显示的条件
    # 更新使用的参数: data 更新的数据 where 条件
    # 访问地址:http://127.0.0.1:5000/index

