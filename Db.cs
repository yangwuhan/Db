using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MySql.Data.MySqlClient;

namespace XglDaService
{
    class Db
    {
        public static string connection_string { get; set; }
        public static string table_refix { get; set; }
        public const string TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

        private string __table_name;
        private List<Dictionary<string, string>> __join;
        private List<string> __where;
        private string __order_field_name;
        private string __order_sort_method;
        private string __fields;

        protected string _last_sql = "";

        public Db(string conn_str)
        {
            if (string.IsNullOrEmpty(conn_str) || string.IsNullOrEmpty(conn_str.Trim()))
                throw new Exception("空连接字符串！");
            conn_str = conn_str.Trim();
            connection_string = conn_str;
            if (string.IsNullOrEmpty(table_refix))
                table_refix = "";
            __table_name = "";
            __join = new List<Dictionary<string, string>>();
            __where = new List<string>();
            __order_field_name = "";
            __order_sort_method = "";
            __fields = "*";
        }

        public static Db Name(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new Exception("空表名！");
            Db ms = new Db();
            ms.__table_name = table_refix + name;
            return ms;
        }

        public static Db Table(string table_name)
        {
            if (string.IsNullOrEmpty(table_name))
                throw new Exception("空表名！");
            Db ms = new Db();
            ms.__table_name = table_name;
            return ms;
        }

        public Db Join(string table_name, string join_condition, string join_method)
        {
            if (string.IsNullOrEmpty(table_name))
                throw new Exception("空表名！");
            if (string.IsNullOrEmpty(join_condition))
                throw new Exception("空条件！");
            if (string.IsNullOrEmpty(join_method))
                throw new Exception("空方式！");
            Dictionary<string, string> dic = new Dictionary<string, string>();
            dic.Add("table_name", table_name);
            dic.Add("join_condition", join_condition);
            dic.Add("join_method", join_method);
            __join.Add(dic);
            return this;
        }

        public Db Where(string where_condition)
        {
            if (string.IsNullOrEmpty(where_condition))
                throw new Exception("空条件！");
            __where.Add(where_condition);
            return this;
        }

        public Db Order(string field_name, string sort_method)
        {
            if (string.IsNullOrEmpty(field_name))
                throw new Exception("空字段！");
            if (string.IsNullOrEmpty(sort_method))
                throw new Exception("空排序方式！");
            __order_field_name = field_name;
            __order_sort_method = sort_method;
            return this;
        }

        public Db Field(string fields)
        {
            if (string.IsNullOrEmpty(fields))
                throw new Exception("空字段！");
            __fields = fields;
            return this;
        }

        public Dictionary<string, object> Find()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
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

        public List<Dictionary<string, object>> Select()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
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

        public int Delete()
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置删除where条件！");
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

        public int Update(Dictionary<string, string> data)
        {
            if (string.IsNullOrEmpty(__table_name))
                throw new Exception("空表名！");
            if (__where.Count == 0)
                throw new Exception("没有设置更新的where条件！");
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
    }
}
