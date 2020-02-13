using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tz
{
    public enum EDbType
    {
        MYSQL = 1,          //MySQL
        SQLITE = 2,         //SQLite
        SQLSERVER2005 = 3,  //SQLServer2005和SQLServer2008
        SQLSERVER2012 = 4  //SQLServer2012以上版本（含2012）
    }
}
