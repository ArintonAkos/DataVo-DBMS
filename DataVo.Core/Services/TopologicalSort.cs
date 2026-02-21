using DataVo.Core.Models.Statement;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Statements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static DataVo.Core.Models.Statement.JoinModel;

namespace DataVo.Core.Services
{
    internal class TopologicalSort
    {
        private Stack<Column> Stack { get; set; }
        private Dictionary<Column, List<Column>> AdjacencyList { get; set; }

        public TopologicalSort()
        {
            Stack = new Stack<Column>();
            AdjacencyList = new Dictionary<Column, List<Column>>();
        }

        public void AddEdge(Column source, Column destination)
        {
            if (!AdjacencyList.ContainsKey(source))
            {
                AdjacencyList[source] = new List<Column>();
            }

            AdjacencyList[source].Add(destination);
        }

        public void Sort()
        {
            HashSet<Column> visited = new();

            foreach (var node in AdjacencyList.Keys)
            {
                SortUtil(node, visited);
            }
        }

        private void SortUtil(Column node, HashSet<Column> visited)
        {
            if (!visited.Contains(node))
            {
                visited.Add(node);

                if (AdjacencyList.ContainsKey(node))
                {
                    foreach (var childNode in AdjacencyList[node])
                    {
                        SortUtil(childNode, visited);
                    }
                }

                Stack.Push(node);
            }
        }

        public List<Column> GetSorted()
        {
            return Stack.ToList();
        }
    }

}
