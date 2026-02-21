using System;
using System.Collections.Generic;
using DataVo.Core.BTree;
using DataVo.Core.BTree.Core;

namespace Server.Benchmarks;

public class DataGenerator
{
    public static (Dictionary<string, Dictionary<string, dynamic>> tableContent, IIndex index) GenerateData(int rowCount)
    {
        var tableContent = new Dictionary<string, Dictionary<string, dynamic>>();
        var index = new JsonBTreeIndex();
        var rand = new Random(42);

        for (int i = 0; i < rowCount; i++)
        {
            string rowId = i.ToString();
            int age = rand.Next(18, 65);

            var row = new Dictionary<string, dynamic>
            {
                { "Id", i },
                { "Name", $"Employee_{i}" },
                { "Age", age }
            };

            tableContent[rowId] = row;
            index.Insert(age.ToString(), rowId);
        }

        return (tableContent, index);
    }
}
