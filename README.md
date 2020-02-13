# Db

一个C#数据库访问包装类

（1）查询

    var recs1 = Db.Table("a").Join("b", "b.id=a.b_id", "left").Where("b.count>10").Order("a", "id", "asc").Field("a.*").Select();
    var rec = Db.Table("a").Join("b", "b.id=a.b_id", "left").Where("b.count>10").Order("a", "id", "asc").Field("a.*").Find();
    var recs2 = Db.QuerySelect("select a.* from a left join b on b.id=a.b_id where b.count>10 order by a.id asc");
    
（2）增加

    Dictionary<string, string> data = new Dictionary<string, string>();

    data.Add("name", "x");
    data.Add("count", "1");
    Db.Table("a").Add(data);
            
（3）修改

    Db.Table("a").Where("id=1").Update(data);
    Db.Table("a").Where("id=1").Dec("count");
    Db.Table("a").Where("id=1").Inc("count");

（4）删除

    Db.Table("a").Where("id=1").Delete();
    
（5）事务

    Db.ExecuteTransaction(cmd => {
        cmd.CommandText = "update a set count=10 where id=1";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "update b set count=10 where id=2";
        cmd.ExecuteNonQuery();
    });
    
