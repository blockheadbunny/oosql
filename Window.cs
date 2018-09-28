using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataFramework {
    public class Window {
        Expression[] PartitionBy { get; set; }
        Expression[] Over { get; set; }
    }
}
