using Server.Models.Statement;
using Server.Parser.Statements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Server.Models.Statement.JoinModel;

namespace Server.Services
{
    internal class TopologicalSort
    {
        private Stack<JoinColumn> Stack { get; set; }
        private Dictionary<JoinColumn, List<JoinColumn>> AdjacencyList { get; set; }

        public TopologicalSort()
        {
            Stack = new Stack<JoinColumn>();
            AdjacencyList = new Dictionary<JoinColumn, List<JoinColumn>>();
        }

        public void AddEdge(JoinColumn source, JoinColumn destination)
        {
            if (!AdjacencyList.ContainsKey(source))
            {
                AdjacencyList[source] = new List<JoinColumn>();
            }

            AdjacencyList[source].Add(destination);
        }

        public void Sort()
        {
            HashSet<JoinColumn> visited = new HashSet<JoinColumn>();

            foreach (var node in AdjacencyList.Keys)
            {
                SortUtil(node, visited);
            }
        }

        private void SortUtil(JoinColumn node, HashSet<JoinColumn> visited)
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

        public List<JoinColumn> GetSorted()
        {
            return Stack.ToList();
        }
    }

}
