using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using MySql.Data.MySqlClient;


namespace Tz
{
    public class Db
    {
        /** 数据库类型
         */
        public static EDbType Type 
        { 
            get 
            { 
                return _type; 
            } 
            set 
            { 
                _type = value; 
                switch(_type)
                {
                    case EDbType.MYSQL:
                    case EDbType.SQLITE:
                        _table_and_field_name_bracket[0] = _table_and_field_name_bracket[1] = "`";
                        break;
                    case EDbType.SQLSERVER2005:
                    case EDbType.SQLSERVER2012:
                        _table_and_field_name_bracket[0] = "[";
                        _table_and_field_name_bracket[1] = "]";
                        break;
                }
            } 
        }
        protected static EDbType _type = EDbType.MYSQL;
       
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
        //MySQL
        protected static string _connection_string = "Server=127.0.0.1;Database=test; User ID=root;Password=123456;port=3306;CharSet=utf8;pooling=true;";
        //SQLite
        //protected static string _connection_string = "Data Source = db.sqlite3";
        //SQL Server
        //protected static string _connection_string = "Data Source = 127.0.0.1 ; Initial Catalog = test ; User ID = sa ; Pwd = 123456 ; ";

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

        /** 字段名括号
         */
        protected static string[] _table_and_field_name_bracket = new string[2] { "`", "`" };

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
        private List<KeyValuePair<string,string>> __order; //ORDER，字段名已经添加了表名、字段名括号以及表名前缀
        private string __fields = "*"; //SELECT字段
        private int __limit_offset = 0;//LIMIT字段，__limit_count为0代表未设置
        private int __limit_count = 0;//LIMIT字段，0代表未设置

        /** 构造函数
         */ 
        private Db()
        {
            if (string.IsNullOrEmpty(_connection_string))
                throw new Exception("空连接字符串！");
            __table_name = "";
            __join = new List<Dictionary<string, string>>();
            __where = new List<string>();
            __order = new List<KeyValuePair<string, string>>();
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
         *  table_name : 字段所在的表名，可以为null或者""
         *  field_name : 字段名（纯字段名，不能带表名前缀）
         *  order_method : 排序方式，只能为ASC或者DESC，不区分大小写
         */
        public Db Order(string table_name, string field_name, string order_method)
        {
            string tb_name = null;
            if (string.IsNullOrEmpty(table_name) || string.IsNullOrEmpty(table_name.Trim()))
                tb_name = null;
            else
                tb_name = table_name.Trim();

            string fd_name = "";
            if (string.IsNullOrEmpty(field_name) || string.IsNullOrEmpty(field_name.Trim()))
                throw new Exception("空字段！");
            fd_name = field_name.Trim();

            string fd = "";
            if (string.IsNullOrEmpty(tb_name) || string.IsNullOrEmpty(tb_name.Trim()))
                fd = _table_and_field_name_bracket[0] + fd_name + _table_and_field_name_bracket[1];
            else
                fd = _table_and_field_name_bracket[0] + tb_name + _table_and_field_name_bracket[1] +
                    "." + _table_and_field_name_bracket[0] + fd_name + _table_and_field_name_bracket[1];

            if (string.IsNullOrEmpty(order_method) || string.IsNullOrEmpty(order_method.Trim()))
                throw new Exception("空排序方式！");
            order_method = order_method.Trim();
            string em = order_method.ToUpper();
            if(em != "ASC" && em != "DESC")
                throw new Exception("排序方式有误，只能为“ASC”或者“DESC”！");

            __order.Add(new KeyValuePair<string, string>(fd, em));

            return this;
        }

        /** LIMIT 子句
         * offset >= 0，count >0
         */
        public Db Limit(int offset, int count)
        {
            if (offset < 0 || count <= 0)
                throw new Exception("参数错误！");
            __limit_offset = offset;
            __limit_count = count;
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
         *  返回：未找到返回null，找到返回一条记录；
         */
        public Dictionary<string, object> Find()
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(__fields))
                __fields = "*";
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT ");
            if (_type == EDbType.SQLSERVER2005 || _type == EDbType.SQLSERVER2012)
                sb.Append(" TOP 1 ");
            sb.Append(__fields).Append(" FROM ");
            sb.Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1]);
            if (__join.Count > 0)
            {
                __join.ForEach((Dictionary<string, string> dic) =>
                {
                    sb.Append(" ").Append(dic["join_method"]).Append(" JOIN ")
                        .Append(_table_and_field_name_bracket[0]).Append(dic["table_name"]).Append(_table_and_field_name_bracket[1])
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
            if (__order.Count > 0)
            {
                sb.Append(" ORDER BY ");
                for (int i = 0; i < __order.Count; ++i)
                {
                    var kv = __order[i];
                    if (i != 0)
                        sb.Append(" , ");
                    sb.Append(" " + kv.Key + " " + kv.Value + " ");
                }
                sb.Append(" ");
            }
            if(_type != EDbType.SQLSERVER2005 && _type != EDbType.SQLSERVER2012)
                sb.Append(" LIMIT 0,1 ");

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Find<MySqlConnection, MySqlCommand, MySqlDataReader>(sb.ToString());
                case EDbType.SQLITE:
                    return _Find<SQLiteConnection, SQLiteCommand, SQLiteDataReader>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Find<SqlConnection, SqlCommand, SqlDataReader>(sb.ToString());
                default:
                    return null;
            }
        }
        protected Dictionary<string, object> _Find<TConnection, TCommand, TDataReader>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
            where TDataReader : DbDataReader, IDisposable
        {
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sql;
                        _last_sql = sql;
                        TDataReader sr = cmd.ExecuteReader() as TDataReader;
                        if (sr.Read())
                        {
                            var ret = new Dictionary<string, object>();
                            for (int i = 0; i < sr.FieldCount; ++i)
                            {
                                string field_name = sr.GetName(i);
                                if (ret.ContainsKey(field_name))
                                {
                                    bool b_fix = false;
                                    for (int fix = 1; fix <= 100; ++fix)
                                    {
                                        string tmp = field_name + fix.ToString();
                                        if (!ret.ContainsKey(tmp))
                                        {
                                            field_name = tmp;
                                            b_fix = true;
                                            break;
                                        }
                                    }
                                    if (!b_fix)
                                        throw new Exception("相同字段名超过100个");
                                }
                                ret.Add(field_name, sr.GetValue(i));
                            }
                            return ret;
                        }
                        else
                            return null;
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
        }

        /** 查询
         * 返回：返回List对象，一条记录都没有，也返回List对象
         */
        public List<Dictionary<string, object>> Select()
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(__fields))
                __fields = "*";
            StringBuilder sb = new StringBuilder();
            if (_type == EDbType.SQLSERVER2005 && __limit_count > 0)
            {
                if (__order.Count == 0)
                    throw new Exception("SQLServer2005和SQLServer2008的分页查询SQL语句必须带有Order子句");
                StringBuilder sb_pre = new StringBuilder();
                sb_pre.Append("SELECT ").Append(__fields).Append(",ROW_NUMBER() OVER (");
                sb_pre.Append(" ORDER BY ");
                for (int i = 0; i < __order.Count; ++i)
                {
                    var kv = __order[i];
                    if (i != 0)
                        sb_pre.Append(" , ");
                    sb_pre.Append(" " + kv.Key + " " + kv.Value + " ");
                }
                sb_pre.Append(" ) AS _RowNum ").Append(" FROM ")
                   .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1]);
                if (__join.Count > 0)
                {
                    __join.ForEach((Dictionary<string, string> dic) =>
                    {
                        sb_pre.Append(" ").Append(dic["join_method"]).Append(" JOIN ")
                            .Append(_table_and_field_name_bracket[0]).Append(dic["table_name"]).Append(_table_and_field_name_bracket[1])
                            .Append(" ON ").Append(dic["join_condition"]);
                    });
                }
                if (__where.Count > 0)
                {
                    sb_pre.Append(" WHERE ");
                    for (int i = 0; i < __where.Count; ++i)
                    {
                        if (i != 0)
                            sb_pre.Append(" AND ");
                        sb_pre.Append("(").Append(__where[i]).Append(")");

                    }
                    sb_pre.Append(" ");
                }
                sb = new StringBuilder();
                sb.Append("SELECT a.* FROM ( ").Append(sb_pre.ToString()).Append(" ) a WHERE a._RowNum BETWEEN ")
                    .Append(__limit_offset + 1).Append(" AND ").Append(__limit_offset + __limit_count).Append(" ");
            }
            else
            {
                sb.Append("SELECT ").Append(__fields).Append(" FROM ")
                    .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1]);
                if (__join.Count > 0)
                {
                    __join.ForEach((Dictionary<string, string> dic) =>
                    {
                        sb.Append(" ").Append(dic["join_method"]).Append(" JOIN ")
                            .Append(_table_and_field_name_bracket[0]).Append(dic["table_name"]).Append(_table_and_field_name_bracket[1])
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
                if (__order.Count > 0)
                {
                    sb.Append(" ORDER BY ");
                    for (int i = 0; i < __order.Count; ++i)
                    {
                        var kv = __order[i];
                        if (i != 0)
                            sb.Append(" , ");
                        sb.Append(" " + kv.Key + " " + kv.Value + " ");
                    }
                    sb.Append(" ");
                }
                if (__limit_count > 0)
                {
                    if (_type == EDbType.MYSQL || _type == EDbType.SQLITE)
                        sb.Append(" LIMIT ").Append(__limit_offset).Append(",").Append(__limit_count).Append(" ");
                    else if (_type == EDbType.SQLSERVER2012)
                        sb.Append(" OFFSET ").Append(__limit_offset).Append(" ROWS FETCH NEXT ").Append(__limit_count).Append(" ROWS ONLY ");
                }
            }
            #endregion

            switch(_type)
            {
                case EDbType.MYSQL:
                    return _Select<MySqlConnection, MySqlCommand, MySqlDataReader>(sb.ToString());
                case EDbType.SQLITE:
                    return _Select<SQLiteConnection, SQLiteCommand, SQLiteDataReader>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Select<SqlConnection, SqlCommand, SqlDataReader>(sb.ToString());
                default:
                    return new List<Dictionary<string, object>>();
            }
        }
        protected List<Dictionary<string, object>> _Select<TConnection, TCommand, TDataReader>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
            where TDataReader : DbDataReader, IDisposable
        {
            List<Dictionary<string, object>> ret = new List<Dictionary<string, object>>();
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sql;
                        _last_sql = sql;
                        TDataReader sr = cmd.ExecuteReader() as TDataReader;
                        while (sr.Read())
                        {
                            Dictionary<string, object> dic = new Dictionary<string, object>();
                            for (int i = 0; i < sr.FieldCount; ++i)
                            {
                                string field_name = sr.GetName(i);
                                if (dic.ContainsKey(field_name))
                                {
                                    bool b_fix = false;
                                    for (int fix = 1; fix <= 100; ++fix)
                                    {
                                        string tmp = field_name + fix.ToString();
                                        if (!dic.ContainsKey(tmp))
                                        {
                                            field_name = tmp;
                                            b_fix = true;
                                            break;
                                        }
                                    }
                                    if (!b_fix)
                                        throw new Exception("相同字段名超过100个");
                                }
                                dic.Add(field_name, sr.GetValue(i));
                            }
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
        }

        /** 删除记录（必须设置WHERE条件才允许删除）
         *  返回：受影响的行数
         */
        public int Delete()
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置WHERE条件！");
            StringBuilder sb = new StringBuilder();
            sb.Append("DELETE FROM ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1]);
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

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Delete<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Delete<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Delete<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Delete<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sql;
                        _last_sql = sql;
                        return cmd.ExecuteNonQuery();
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
        }

        /** 添加记录
         *  返回：受影响的行数，MYSQL返回的是插入的ID?【要求数据表必须有自增的id字段】 
         */
        public int Add(Dictionary<string, string> data)
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (data.Count == 0)
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("INSERT INTO ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1])
                .Append("(");
            List<string> keys = new List<string>(data.Keys);
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string key = keys[i];
                sb.Append(_table_and_field_name_bracket[0]).Append(key).Append(_table_and_field_name_bracket[1]);
            }
            sb.Append(") VALUES(");
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string val = data[keys[i]];
                if(val == null)
                {
                    sb.Append("null");
                }
                else
                {
                    val = _ValidValue(val);
                    sb.Append("'").Append(val).Append("'");
                }
            }
            sb.Append(")");

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Add<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Add<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Add<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Add<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            int ret = 0;
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;

                        if(_type == EDbType.SQLSERVER2005 || _type == EDbType.SQLSERVER2012)
                        {
                            string s = @"SET NOCOUNT ON ;";
                            s += sql + " ;";
                            s += @"SELECT ThisID = @@Identity;";
                            s += @"SET NOCOUNT OFF;";
                            cmd.CommandText = s;
                            _last_sql = s;
                            var o = cmd.ExecuteScalar();
                            ret = int.Parse(o.ToString());
                        }
                        else if(_type == EDbType.MYSQL)
                        {
                            cmd.CommandText = sql;
                            _last_sql = sql;
                            ret = cmd.ExecuteNonQuery();
                            ret = (int)(cmd as MySqlCommand).LastInsertedId;
                        }
                        else
                        {
                            string s1 = sql;
                            cmd.CommandText = s1;
                            cmd.ExecuteNonQuery();
                            string s2 = "select last_insert_rowid() from " + _table_and_field_name_bracket[0] + __table_name + _table_and_field_name_bracket[1];
                            _last_sql = s1 + ";" + s2 + ";";
                            cmd.CommandText = s2;
                            var o = cmd.ExecuteScalar();
                            ret = int.Parse(o.ToString());
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
            return ret;
        }

        /** 更新记录（必须设置WHERE条件）
         *  返回：受影响的行数
         */
        public int Update(Dictionary<string, string> data)
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置WHERE条件！");
            if (data.Count == 0)
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1])
                .Append(" SET ");
            List<string> keys = new List<string>(data.Keys);
            for (int i = 0; i < keys.Count; ++i)
            {
                if (i != 0)
                    sb.Append(",");
                string key = keys[i];
                sb.Append(_table_and_field_name_bracket[0]).Append(key).Append(_table_and_field_name_bracket[1])
                    .Append("=");
                if(data[key] == null)
                {
                    sb.Append("null");
                }
                else
                {
                    sb.Append("'")
                        .Append(_ValidValue(data[key]))
                        .Append("'");
                }
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

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Update<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Update<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Update<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Update<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            int ret = 0;
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sql;
                        _last_sql = sql;
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

        /** 指定字段值加1
         *  返回：受影响的行数
         */
        public int Inc(string field_name)
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置更新的where条件！");
            if (string.IsNullOrEmpty(field_name))
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1])
                .Append(" SET ")
                .Append(_table_and_field_name_bracket[0]).Append(field_name).Append(_table_and_field_name_bracket[1])
                .Append("=")
                .Append(_table_and_field_name_bracket[0]).Append(field_name).Append(_table_and_field_name_bracket[1])
                .Append("+1");
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

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Inc<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Inc<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Inc<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Inc<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            int ret = 0;
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        _last_sql = sql;
                        cmd.CommandText = sql;
                        ret = cmd.ExecuteNonQuery();
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception("数据库操作失败（异常：" + ex.Message + "）！");
                }
                finally
                {
                    con.Close();
                }
            }
            return ret;
        }

        /** 指定字段值减1
         *  返回：受影响的行数
         */
        public int Dec(string field_name)
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置更新的where条件！");
            if (string.IsNullOrEmpty(field_name))
                throw new Exception("无字段！");
            StringBuilder sb = new StringBuilder();
            sb.Append("UPDATE ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1])
                .Append(" SET ")
                .Append(_table_and_field_name_bracket[0]).Append(field_name).Append(_table_and_field_name_bracket[1])
                .Append("=")
                .Append(_table_and_field_name_bracket[0]).Append(field_name).Append(_table_and_field_name_bracket[1])
                .Append("-1");
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

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Dec<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Dec<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Dec<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Dec<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            int ret = 0;
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        _last_sql = sql;
                        cmd.CommandText = sql;
                        ret = cmd.ExecuteNonQuery();
                    }
                }
                catch (System.Exception ex)
                {
                    throw new Exception("数据库操作失败（异常：" + ex.Message + "）！");
                }
                finally
                {
                    con.Close();
                }
            }
            return ret;
        }

        /** 统计满足查询条件的行数
         *  返回：行数
         */
        public int Count()
        {
            #region 构造SQL语句

            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(__fields))
                __fields = "*";
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT COUNT( ").Append(__fields).Append(" ) AS cnt FROM ")
                .Append(_table_and_field_name_bracket[0]).Append(__table_name).Append(_table_and_field_name_bracket[1]);
            if (__join.Count > 0)
            {
                __join.ForEach((Dictionary<string, string> dic) =>
                {
                    sb.Append(" ").Append(dic["join_method"]).Append(" JOIN ")
                        .Append(_table_and_field_name_bracket[0]).Append(dic["table_name"]).Append(_table_and_field_name_bracket[1])
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

            #endregion

            switch (_type)
            {
                case EDbType.MYSQL:
                    return _Count<MySqlConnection, MySqlCommand>(sb.ToString());
                case EDbType.SQLITE:
                    return _Count<SQLiteConnection, SQLiteCommand>(sb.ToString());
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _Count<SqlConnection, SqlCommand>(sb.ToString());
                default:
                    return 0;
            }
        }
        protected int _Count<TConnection, TCommand>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
        {
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.CommandText = sql;
                        _last_sql = sql;
                        object a = cmd.ExecuteScalar();
                        if (a is DBNull)
                            return 0;
                        else
                            return int.Parse(a.ToString());
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
        }

        /** 使用SQL语句查询
         *  返回：List对象，一条记录都没有，也返回List对象。
         */
        public static List<Dictionary<string, object>> QuerySelect(string sql)            
        {
            switch (_type)
            {
                case EDbType.MYSQL:
                    return _QuerySelect<MySqlConnection, MySqlCommand, MySqlDataReader>(sql);
                case EDbType.SQLITE:
                    return _QuerySelect<SQLiteConnection, SQLiteCommand, SQLiteDataReader>(sql);
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    return _QuerySelect<SqlConnection, SqlCommand, SqlDataReader>(sql);
                default:
                    return new List<Dictionary<string, object>>();
            }
        }
        protected static List<Dictionary<string, object>> _QuerySelect<TConnection, TCommand, TDataReader>(string sql)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
            where TDataReader : DbDataReader, IDisposable
        {
            List<Dictionary<string, object>> ret = new List<Dictionary<string, object>>();
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        _last_sql = sql;
                        cmd.CommandText = sql;
                        TDataReader sr = cmd.ExecuteReader() as TDataReader;
                        while (sr.Read())
                        {
                            Dictionary<string, object> dic = new Dictionary<string, object>();
                            for (int i = 0; i < sr.FieldCount; ++i)
                            {
                                string field_name = sr.GetName(i);
                                if (dic.ContainsKey(field_name))
                                {
                                    bool b_fix = false;
                                    for (int fix = 1; fix <= 100; ++fix)
                                    {
                                        string tmp = field_name + fix.ToString();
                                        if (!dic.ContainsKey(tmp))
                                        {
                                            field_name = tmp;
                                            b_fix = true;
                                            break;
                                        }
                                    }
                                    if (!b_fix)
                                        throw new Exception("相同字段名超过100个");
                                }
                                dic.Add(field_name, sr.GetValue(i));
                            }
                            ret.Add(dic);
                        }
                        return ret;
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("数据库操作失败（异常：" + ex.Message + "）！SQL：" + sql);
                }
                finally
                {
                    con.Close();
                }
            }
        }

        /** 执行事务
         */
        public static void ExecuteTransaction(Action<DbCommand> action)
        {
            switch (_type)
            {
                case EDbType.MYSQL:
                    _ExecuteTransaction<MySqlConnection, MySqlCommand, MySqlTransaction>(action);
                    break;
                case EDbType.SQLITE:
                    _ExecuteTransaction<SQLiteConnection, SQLiteCommand, SQLiteTransaction>(action);
                    break;
                case EDbType.SQLSERVER2005:
                case EDbType.SQLSERVER2012:
                    _ExecuteTransaction<SqlConnection, SqlCommand, SqlTransaction>(action);
                    break;
            }
        }
        protected static void _ExecuteTransaction<TConnection, TCommand, TTransaction>(Action<DbCommand> action)
            where TConnection : DbConnection, IDisposable, new()
            where TCommand : DbCommand, IDisposable, new()
            where TTransaction : DbTransaction
        {
            if (action == null)
                return;
            using (TConnection con = new TConnection())
            {
                con.ConnectionString = connection_string;
                con.Open();
                TTransaction transaction = con.BeginTransaction() as TTransaction;
                try
                {
                    using (TCommand cmd = new TCommand())
                    {
                        cmd.Connection = con;
                        cmd.Transaction = transaction;
                        cmd.CommandType = System.Data.CommandType.Text;
                        action(cmd);
                    }
                    transaction.Commit();//提交事务
                }
                catch (System.Exception ex)
                {
                    transaction.Rollback();//事务回滚
                    throw ex;
                }
                finally
                {
                    con.Close();
                }
            }
        }

        #endregion

        /** 转义字符处理
         */
        protected static string _ValidValue(string v)
        {
            if (string.IsNullOrEmpty(v))
                return v;
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < v.Length; ++i)
            {
                char c = v[i];
                sb.Append(c);
                if(_type == EDbType.MYSQL)
                {
                    if (c == '\\')
                        sb.Append(@"\");
                    else if (c == '\'')
                        sb.Append(@"'");
                }
                else
                {
                    if (c == '\'')
                        sb.Append(@"'");
                }
            }
            return sb.ToString();
        }

        /** 判断字段名是否包含表前缀
         */ 
        protected static bool _IsFieldNameContainTableName(String field_name)
        {
            return (field_name.IndexOf('.') > 0);
        }
    }
}
