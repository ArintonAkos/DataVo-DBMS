using DataVo.Core.Models.Statement.Utils;

namespace DataVo.Core.Services
{
    internal class TopologicalSort
    {
        private Stack<Column> Stack { get; set; }
        private Dictionary<Column, List<Column>> AdjacencyList { get; set; }

        public TopologicalSort()
        {
            Stack = new Stack<Column>();
            AdjacencyList = [];
        }

        public void AddEdge(Column source, Column destination)
        {
            if (!AdjacencyList.ContainsKey(source))
            {
                AdjacencyList[source] = [];
            }

            AdjacencyList[source].Add(destination);
        }

        public void Sort()
        {
            HashSet<Column> visited = [];

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
