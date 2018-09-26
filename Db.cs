﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySql.Data.MySqlClient;

namespace Tz
{
    public class Db
    {
        /** 连接字符串
         */ 
        public static string connection_string
        {
            get { return _connection_string; }
            set
            {
                if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(value.Trim()))
                    throw new Exception("空字符串！");
                _connection_string = value.Trim();
            }
        }
        protected static string _connection_string = "Server=127.0.0.1;Database=test; User ID=root;Password=123456;port=3306;CharSet=utf8;pooling=true;";

        /** 表前缀
         */ 
        public static string table_prefix
        {
            get { return _table_prefix; }
            set
            {
                if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(value.Trim()))
                    _table_prefix = "";
                else
                    _table_prefix = value.Trim();
            }
        }
        protected static string _table_prefix = "";

        /** DATETIME字段格式化字符串
         */
        public static string format_datetime
        {
            get { return _format_datetime; }
            set
            {
                if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(value.Trim()))
                    throw new Exception("空字符串！");
                _format_datetime = value.Trim();
            }
        }
        protected static string _format_datetime = "yyyy-MM-dd HH:mm:ss";

        /** 上次执行的SQL语句
         */ 
        public static string last_sql
        {
            get { return _last_sql; }
        }
        protected static string _last_sql = "";

        /** 查询使用的临时变量
         */
        private string __table_name = ""; //FROM表名
        private List<Dictionary<string, string>> __join; //JOIN条件
        private List<string> __where; //WHERE条件
        private string __order_field_name; //ORDER字段
        private string __order_sort_method; //ORDER方法
        private string __fields = "*"; //SELECT字段

        /** 构造函数
         */ 
        private Db()
        {
            if (string.IsNullOrEmpty(_connection_string))
                throw new Exception("空连接字符串！");
            __table_name = "";
            __join = new List<Dictionary<string, string>>();
            __where = new List<string>();
            __order_field_name = "";
            __order_sort_method = "";
            __fields = "*";
        }

        #region 参数

        /** FROM 表名（不带前缀）
         */
        public static Db Name(string name)
        {
            if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(name))
                throw new Exception("空表名！");
            name = name.Trim();
            Db ms = new Db();
            ms.__table_name = _table_prefix + name;
            return ms;
        }

        /** FROM 完整表名（带前缀）
         */
        public static Db Table(string table_name)
        {
            if (string.IsNullOrEmpty(table_name) || string.IsNullOrEmpty(table_name))
                throw new Exception("空表名！");
            table_name = table_name.Trim();
            Db ms = new Db();
            ms.__table_name = table_name;
            return ms;
        }

        /** JOIN 完整表名
         */ 
        public Db Join(string table_name, string join_condition, string join_method)
        {
            if (string.IsNullOrEmpty(table_name) || string.IsNullOrEmpty(table_name))
                throw new Exception("空表名！");
            table_name = table_name.Trim();
            if (string.IsNullOrEmpty(join_condition) || string.IsNullOrEmpty(join_condition))
                throw new Exception("空条件！");
            join_condition = join_condition.Trim();
            if (string.IsNullOrEmpty(join_method) || string.IsNullOrEmpty(join_method))
                throw new Exception("空方式！");
            join_method = join_method.Trim();
            Dictionary<string, string> dic = new Dictionary<string, string>();
            dic.Add("table_name", table_name);
            dic.Add("join_condition", join_condition);
            dic.Add("join_method", join_method);
            __join.Add(dic);
            return this;
        }

        /** WHERE 条件
         */ 
        public Db Where(string where_condition)
        {
            if (string.IsNullOrEmpty(where_condition) || string.IsNullOrEmpty(where_condition.Trim()))
                throw new Exception("空条件！");
            where_condition = where_condition.Trim();
            __where.Add(where_condition);
            return this;
        }

        /** ORDER 子句
         */ 
        public Db Order(string field_name, string order_method)
        {
            if (string.IsNullOrEmpty(field_name) || string.IsNullOrEmpty(field_name.Trim()))
                throw new Exception("空字段！");
            field_name = field_name.Trim();
            if (string.IsNullOrEmpty(order_method) || string.IsNullOrEmpty(order_method.Trim()))
                throw new Exception("空排序方式！");
            order_method = order_method.Trim();
            string em = order_method.ToUpper();
            if(em != "ASC" && em != "DESC")
                throw new Exception("排序方式有误！");
            __order_field_name = field_name;
            __order_sort_method = order_method;
            return this;
        }

        /** SELECT 哪些字段
         */ 
        public Db Field(string fields)
        {
            if (string.IsNullOrEmpty(fields) || string.IsNullOrEmpty(fields.Trim()))
                throw new Exception("空字段！");
            fields = fields.Trim();
            __fields = fields;
            return this;
        }

        #endregion

        #region 动作

        /** 查询第一条记录
         */ 
        public Dictionary<string, object> Find()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(__fields))
                __fields = "*";
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ").Append(__fields).Append(" FROM `").Append(__table_name).Append("`");
            if (__join.Count > 0)
            {
                __join.ForEach((Dictionary<string, string> dic) =>
                {
                    sb.Append(" ").Append(dic["join_method"]).Append(" JOIN `").Append(dic["table_name"]).Append("`")
                        .Append(" ON ").Append(dic["join_condition"]);
                });
            }
            if (__where.Count > 0)
            {
                sb.Append(" WHERE ");
                for (int i = 0; i < __where.Count; ++i)
                {
                    if (i != 0)
                        sb.Append(" AND ");
                    sb.Append("(").Append(__where[i]).Append(")");

                }
                sb.Append(" ");
            }
            if (!string.IsNullOrEmpty(__order_field_name) && !string.IsNullOrEmpty(__order_sort_method))
                sb.Append(" ORDER BY `").Append(__order_field_name).Append("` ").Append(__order_sort_method);
            sb.Append(" LIMIT 0,1 ");
            Dictionary<string, object> ret = new Dictionary<string, object>();
            using (MySqlConnection con = new MySqlConnection(connection_string))
            {
                con.Open();
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sb.ToString();
                        _last_sql = sb.ToString();
                        MySqlDataReader sr = cmd.ExecuteReader();
                        if (sr.Read())
                        {
                            for (int i = 0; i < sr.FieldCount; ++i)
                                ret.Add(sr.GetName(i), sr.GetValue(i));
                        }
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception(ex.Message + "（sql:" + _last_sql + "）");
                }
                finally
                {
                    con.Close();
                }
            }
            return (ret.Count > 0 ? ret : null);
        }

        /** 查询
         */ 
        public List<Dictionary<string, object>> Select()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(__fields))
                __fields = "*";
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ").Append(__fields).Append(" FROM `").Append(__table_name).Append("`");
            if (__join.Count > 0)
            {
                __join.ForEach((Dictionary<string, string> dic) =>
                {
                    sb.Append(" ").Append(dic["join_method"]).Append(" JOIN `").Append(dic["table_name"]).Append("`")
                        .Append(" ON ").Append(dic["join_condition"]);
                });
            }
            if (__where.Count > 0)
            {
                sb.Append(" WHERE ");
                for (int i = 0; i < __where.Count; ++i)
                {
                    if (i != 0)
                        sb.Append(" AND ");
                    sb.Append("(").Append(__where[i]).Append(")");

                }
                sb.Append(" ");
            }
            if (!string.IsNullOrEmpty(__order_field_name) && !string.IsNullOrEmpty(__order_sort_method))
                sb.Append(" ORDER BY `").Append(__order_field_name).Append("` ").Append(__order_sort_method);
            List<Dictionary<string, object>> ret = new List<Dictionary<string, object>>();
            using (MySqlConnection con = new MySqlConnection(connection_string))
            {
                con.Open();
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sb.ToString();
                        _last_sql = sb.ToString();
                        MySqlDataReader sr = cmd.ExecuteReader();
                        while (sr.Read())
                        {
                            Dictionary<string, object> dic = new Dictionary<string, object>();
                            for (int i = 0; i < sr.FieldCount; ++i)
                                dic.Add(sr.GetName(i), sr.GetValue(i));
                            ret.Add(dic);
                        }
                        return ret;
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception(ex.Message + "（sql:" + _last_sql + "）");
                }
                finally
                {
                    con.Close();
                }
            }
            return (ret.Count > 0 ? ret : null);
        }

        /** 删除记录（必须设置WHERE条件才允许删除）
         *  返回：受影响的行数
         */ 
        public int Delete()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置WHERE条件！");
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM `").Append(__table_name).Append("`");
            if (__where.Count > 0)
            {
                sb.Append(" WHERE ");
                for (int i = 0; i < __where.Count; ++i)
                {
                    if (i != 0)
                        sb.Append(" AND ");
                    sb.Append("(").Append(__where[i]).Append(")");

                }
                sb.Append(" ");
            }
            int ret = 0;
            using (MySqlConnection con = new MySqlConnection(connection_string))
            {
                con.Open();
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sb.ToString();
                        _last_sql = sb.ToString();
                        ret = cmd.ExecuteNonQuery();
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception(ex.Message + "（sql:" + _last_sql + "）");
                }
                finally
                {
                    con.Close();
                }
            }
            return ret;
        }

        /** 添加记录
         *  返回：受影响的行数
         */ 
        public int Add(Dictionary<string, string> data)
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (data.Count == 0)
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO `").Append(__table_name).Append("`(");
            List<string> keys = new List<string>(data.Keys);
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string key = keys[i];
                sb.Append("`").Append(key).Append("`");
            }
            sb.Append(") VALUES(");
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string val = data[keys[i]];
                sb.Append("'").Append(val).Append("'");
            }
            sb.Append(")");
            int ret = 0;
            using (MySqlConnection con = new MySqlConnection(connection_string))
            {
                con.Open();
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sb.ToString();
                        _last_sql = sb.ToString();
                        ret = cmd.ExecuteNonQuery();
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception(ex.Message + "（sql:" + _last_sql + "）");
                }
                finally
                {
                    con.Close();
                }
            }
            return ret;
        }

        /** 更改记录（必须设置WHERE条件）
         */ 
        public int Update(Dictionary<string, string> data)
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置WHERE条件！");
            if (data.Count == 0)
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE `").Append(__table_name).Append("` SET ");
            List<string> keys = new List<string>(data.Keys);
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string key = keys[i];
                sb.Append("`").Append(key).Append("`='").Append(data[key]).Append("'");
            }
            if (__where.Count > 0)
            {
                sb.Append(" WHERE ");
                for (int i = 0; i < __where.Count; ++i)
                {
                    if (i != 0)
                        sb.Append(" AND ");
                    sb.Append("(").Append(__where[i]).Append(")");

                }
                sb.Append(" ");
            }
            int ret = 0;
            using (MySqlConnection con = new MySqlConnection(connection_string))
            {
                con.Open();
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sb.ToString();
                        _last_sql = sb.ToString();
                        ret = cmd.ExecuteNonQuery();
                    }
                }
                catch(System.Exception ex)
                {
                    throw new Exception(ex.Message + "（sql:" + _last_sql + "）");
                }
                finally
                {
                    con.Close();
                }
            }
            return ret;
        }

        #endregion
    }
}
